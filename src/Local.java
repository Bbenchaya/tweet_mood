/**
 * Created by asafchelouche on 25/3/16.
 */

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.codec.binary.Base64;

import java.io.*;
import java.util.*;

import static java.lang.Thread.sleep;
import static javafx.application.Platform.exit;

public class Local {

    private static final String BUCKET_NAME = "asafbendsp";
    private static final String RESULTS_FILENAME_SUFFIX = "results.txt";
    private static String inputFileName;
    private static String outputFileName;
    private static String objectName;
    private static String id;
    private static final String LINKS_FILENAME_SUFFIX = "links.txt";
    private static final String HEADER = "<html>\n\t<head>\n\t\t<title>DSP 162, assignment 1</title>\n\t</head>\n\t<body>";
    private static final String FOOTER = "\n\t</body>\n</html>";
    private static boolean managerShouldTerminate;
    private static String upstreamURL;
    private static String downstreamURL;
    private static final String URLS_FILENAME = "up-down.txt";
    private static AmazonS3 s3;
    private static AmazonSQS sqs;
    private static AmazonEC2 ec2;


    public static void main(String[] args) throws IOException {
        if (args.length < 3 || args.length > 4) {
            System.out.println("Usage: java -jar <yourjar.jar> <inputFileName.txt> <outputFileName.html> <workersToFileRatio> <terminate>");
            exit();
        }
        inputFileName = args[0];
        outputFileName = args[1];
        int workersToFileRatio = Integer.parseInt(args[2]);
        managerShouldTerminate = args.length == 4 && args[3].equals("terminate");
        id = UUID.randomUUID().toString();
        System.out.println("Local instance id: " + id);
        objectName = id + LINKS_FILENAME_SUFFIX;

        //upload the tweet links list to S3
        s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        System.out.println("Local client running...");
        try {
            System.out.print("Uploading the tweet links file to S3... ");
            File file = new File(System.getProperty("user.dir") + "/" + inputFileName);
            s3.putObject(new PutObjectRequest(BUCKET_NAME, objectName, file));
            System.out.println("Done.");
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }

        //check for a running Manager instance on EC2, if not initialize one
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
        ec2 = new AmazonEC2Client(credentials);
        ec2.setRegion(usEast1);
        sqs = new AmazonSQSClient(credentials);
        sqs.setRegion(usEast1);
        List<String> tagValues = new ArrayList<>();
        tagValues.add("manager");
        Filter tagFilter = new Filter("tag:kind", tagValues);
        List<String> statusValues = new ArrayList<>();
        statusValues.add("running");
        Filter statusFilter = new Filter("instance-state-name", statusValues);
        DescribeInstancesResult filteredInstances = ec2.describeInstances(new DescribeInstancesRequest().withFilters(tagFilter, statusFilter));
        List<Reservation> reservations = filteredInstances.getReservations();
        if (reservations.size() > 0) { // a Manager instance is already running
            // get the URLs file from S3
            System.out.print("Manager instance already running, downloading UPSTREAM\\DOWNSTREAM queues' URLs file from S3... ");
            S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, URLS_FILENAME));
            System.out.println("Done.");
            BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            upstreamURL = br.readLine();
            downstreamURL = br.readLine();
            br.close();
            sendRequestInUpstream();
        }
        else { // create the SQSs and start a manager instance

            System.out.println("No Manager instance currently running.");
            // start 2 SQSs: upstream, downstream
            try {
                // Create the upstream and downstream queues
                System.out.print("Creating upstream queue... ");
                CreateQueueRequest createUpstreamQueueRequest = new CreateQueueRequest("upstream");
                upstreamURL = sqs.createQueue(createUpstreamQueueRequest).getQueueUrl();
                System.out.println("Done.");
                System.out.print("Creating downstream queue... ");
                CreateQueueRequest createDownstreamQueueRequest = new CreateQueueRequest("downstream");
                downstreamURL = sqs.createQueue(createDownstreamQueueRequest).getQueueUrl();
                System.out.println("Done.");

                // create a file that holds the queues' URLs, and upload it to S3 for the manager
                File file = new File(System.getProperty("user.dir") + "/" + URLS_FILENAME);
                FileWriter fw = new FileWriter(file);
                fw.write(upstreamURL + "\n");
                fw.write(downstreamURL + "\n");
                fw.write(Integer.toString(workersToFileRatio) + "\n");
                fw.close();
                System.out.print("Uploading the UPSTREAM\\DOWNSTREAM queues' URLs file to S3... ");
                s3.putObject(new PutObjectRequest(BUCKET_NAME, URLS_FILENAME, file));
                System.out.println("Done.");
            } catch (AmazonServiceException ase) {
                System.out.println("Caught an AmazonServiceException, which means your request made it " +
                        "to Amazon SQS, but was rejected with an error response for some reason.");
                System.out.println("Error Message:    " + ase.getMessage());
                System.out.println("HTTP Status Code: " + ase.getStatusCode());
                System.out.println("AWS Error Code:   " + ase.getErrorCode());
                System.out.println("Error Type:       " + ase.getErrorType());
                System.out.println("Request ID:       " + ase.getRequestId());
            } catch (AmazonClientException ace) {
                System.out.println("Caught an AmazonClientException, which means the client encountered " +
                        "a serious internal problem while trying to communicate with SQS, such as not " +
                        "being able to access the network.");
                System.out.println("Error Message: " + ace.getMessage());
            }

            // start a Manager instance
            try {
                System.out.println("Firing up new Manager instance...");
                RunInstancesRequest request = new RunInstancesRequest("ami-37d0c45d", 1, 1); // upgraded ami: yum updated, java 8
                request.setInstanceType(InstanceType.T2Micro.toString());
                request.setUserData(getUserDataScript());
                IamInstanceProfileSpecification iamInstanceProfileSpecification = new IamInstanceProfileSpecification();
                iamInstanceProfileSpecification.setName("creds");
                request.setIamInstanceProfile(iamInstanceProfileSpecification);
                request.setKeyName("AWS");
                List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
                System.out.println("Launch instances: " + instances);
                CreateTagsRequest createTagRequest = new CreateTagsRequest();
                createTagRequest.withResources(instances.get(0).getInstanceId()).withTags(new Tag("kind", "manager"));
                ec2.createTags(createTagRequest);
                sendRequestInUpstream();
            } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Response Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
            }
        }

        // wait for the manager to run
        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withFilters(tagFilter, statusFilter);
        while (ec2.describeInstances(describeInstancesRequest).getReservations().isEmpty()) {
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Manager instance running.");
        compileResultsToHTML();
    }

    private static void sendRequestInUpstream() {
        sqs.sendMessage(upstreamURL, id + LINKS_FILENAME_SUFFIX);
    }

    private static void compileResultsToHTML() throws IOException {
        // await for the results file, and compile it to HTML
        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(downstreamURL);
        List<String> attributeNames = new LinkedList<>();
        attributeNames.add("ApproximateNumberOfMessages");
        getQueueAttributesRequest.setAttributeNames(attributeNames);
        ReceiveMessageResult receiveMessageResult;
        System.out.println("Awaiting results...");
        while (true) {
            while ((receiveMessageResult = sqs.receiveMessage(downstreamURL)).getMessages().isEmpty()) {
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (receiveMessageResult.toString().contains("done")) {
                List<Message> messages = receiveMessageResult.getMessages();
                for (Message message : messages) {
                    if (message.getBody().contains("done")) {
                        String messageReceiptHandle = message.getReceiptHandle();
                        sqs.changeMessageVisibility(downstreamURL, messageReceiptHandle, 0);
                        sqs.deleteMessage(new DeleteMessageRequest(downstreamURL, messageReceiptHandle));
                        break;
                    }
                }
                break;
            }
        }
        System.out.print("Downloading results file from S3... ");
        S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, id + RESULTS_FILENAME_SUFFIX));
        System.out.println("Done.");
        File output = new File(System.getProperty("user.dir") + "/" + outputFileName);
        FileWriter fw = new FileWriter(output);
        fw.write(HEADER);
        Scanner scanner = new Scanner(new InputStreamReader(object.getObjectContent()));
        scanner.useDelimiter("<delimiter>");

        //tokenize the reservations file and process each token one at a time
        System.out.println("Compiling the results to HTML. Output path: " + System.getProperty("user.dir") + "/" + outputFileName);
        while (scanner.hasNext()) {
            String result = scanner.next();
            // handle the tail after the last delimiter
            if (result.equals("</result>\n"))
                break;
            String tweet = null;
            try {
                tweet = result.substring(result.indexOf("<tweet>") + 7, result.indexOf("</tweet>"));
            }catch (Exception e){
                System.out.println("bad result: " + result);
            }
            String sentiment = result.substring(result.indexOf("<sentiment>") + 11, result.indexOf("</sentiment>"));
            String entities = result.substring(result.indexOf("<entities>") + 10, result.indexOf("</entities>"));
            String fontColor = null;
            switch(sentiment) {
                case "0":
                    fontColor = "#610B0B"; //dark red
                    break;
                case "1":
                    fontColor = "red";
                    break;
                case "2":
                    fontColor = "black";
                    break;
                case "3":
                    fontColor = "#40FF00"; //light green
                    break;
                case "4":
                    fontColor = "#0B3B0B"; //dark green
                    break;
            }
            fw.write("\n\t\t<p>");
            fw.write("<b><font color=\"" + fontColor + "\">");
            fw.write(tweet);
            fw.write("</font></b>");
            fw.write(entities);
            fw.write("</p>");
            fw.flush();
        }
        fw.write(FOOTER);
        fw.flush();
        fw.close();
        scanner.close();
        if (managerShouldTerminate) {
            sqs.sendMessage(upstreamURL, "<id>" + id + "</id>terminate");
            System.out.println("Sent termination message to the Manager.");
        }
        System.out.println("Finished execution, exiting...");
    }

    private static String getUserDataScript(){
        StringBuilder sb = new StringBuilder();
        sb.append("#! /bin/bash\n");
        sb.append("aws s3 cp s3://asafbendsp/Manager.jar Manager.jar\n");
        sb.append("aws s3 cp s3://asafbendsp/aws-sdk-java/lib . --recursive\n");
        sb.append("aws s3 cp s3://asafbendsp/aws-sdk-java/thirdparty/lib . --recursive\n");
        sb.append("jar xf Manager.jar\n");
        sb.append("java -cp .:./* -Xms128m -Xmx768m Manager\n");
        // AWS requires that user data be encoded in base-64
        String str = null;
        try {
            str = new String(Base64.encodeBase64(sb.toString().getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return str;
    }

}