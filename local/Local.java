/**
 * Created by asafchelouche on 25/3/16.
 */

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

import static java.lang.Thread.sleep;
import static javafx.application.Platform.exit;

import java.io.BufferedReader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cognitosync.model.Platform;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;

import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

public class Local {

    private static final String BUCKET_NAME = "asafbendsp";
    private static final String RESULTS_FILE_SUFFIX = "results.txt";
    //    private static final String inputFileName =  "/Users/bbenchaya/Documents/Programming/DSP/tweet_mood/src/tweetLinks.txt";
    private static String inputFileName;
    private static String outputFileName;
    private static String objectName;
    private static String id;
    private static final String LINKS_FILE = "links.txt";
    private static final String HEADER = "<html>\n\t<head>\n\t\t<title>DSP 162, assignment 1</title>\n\t</head>\n\t<body>";
    private static final String FOOTER = "\n\t</body>\n</html>";
    private static boolean managerShouldTerminate;
    private static String upstreamURL;
    private static String downstreamURL;
    private static String URLS_FILENAME = "URLs.txt";
    private static AmazonSQS sqs;


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
        objectName = id + LINKS_FILE;
//        for (String arg : args)
//            System.out.println(arg);
//        System.out.println(managerShouldTerminate);
        Manager manager = null;

        //upload the tweet links list to S3
        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        System.out.println("Local client running...");
        try {
            System.out.print("Uploading the tweet links file to S3... ");
            File file = new File(inputFileName);
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
        //add file link to jobs
//        SQS jobs = new SQS();
//        jobs.queue.add(inputFileName);

//        SQS reservations = new SQS();

        //check if a Manager exists on EC2, if not initialize it
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
        AmazonEC2 ec2 = new AmazonEC2Client(credentials);
        ec2.setRegion(usEast1);
        List<String> tagValues = new ArrayList<>();
        tagValues.add("manager");
        tagValues.add("worker");
        Filter tagFilter = new Filter("tag:kind", tagValues);
        List<String> statusValues = new ArrayList<>();
        statusValues.add("running");
        Filter statusFilter = new Filter("instance-state-name", statusValues);
        DescribeInstancesResult filteredInstances = ec2.describeInstances(new DescribeInstancesRequest().withFilters(tagFilter, statusFilter));
        List<Reservation> reservations = filteredInstances.getReservations();
        if (reservations.size() > 0) { // a Manager instance is already running
            // get the URLs file from S3
            System.out.print("Downloading URLs file from S3... ");
            S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, URLS_FILENAME));
            System.out.println("Done.");
            BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            upstreamURL = br.readLine();
            downstreamURL = br.readLine();
            br.close();
        }
        else { // create the SQSs and start a manager instance
            // start 2 SQSs: upstream, downstream
            sqs = new AmazonSQSClient(credentials);
            sqs.setRegion(usEast1);
            try {
                // Create the upstream and downstream queues
                System.out.print("Creating upstream queue...");
                CreateQueueRequest createUpstreamQueueRequest = new CreateQueueRequest("upstream");
                upstreamURL = sqs.createQueue(createUpstreamQueueRequest).getQueueUrl();
                System.out.println("Done.");
                System.out.print("Creating downstream queue...");
                CreateQueueRequest createDownstreamQueueRequest = new CreateQueueRequest("downstream");
                downstreamURL = sqs.createQueue(createDownstreamQueueRequest).getQueueUrl();
                System.out.println("Done.");

                // create a file that holds the queues' URLs, and upload it to S3 for the manager
                File file = new File(System.getProperty("user.dir") + "/" + URLS_FILENAME);
                FileWriter fw = new FileWriter(file);
                fw.write(upstreamURL + "\n");
                fw.write(downstreamURL + "\n");
                fw.close();
                System.out.print("Uploading the URLs file to S3... ");
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
                RunInstancesRequest request = new RunInstancesRequest("ami-7b2e2611", 1, 1);
                request.setInstanceType(InstanceType.T2Micro.toString());
                List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
                System.out.println("Launch instances: " + instances);
                CreateTagsRequest createTagRequest = new CreateTagsRequest();
                createTagRequest.withResources(instances.get(0).getInstanceId()).withTags(new Tag("kind", "manager"));
                ec2.createTags(createTagRequest);
            } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Response Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());

            }

        }
        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(downstreamURL);
        ReceiveMessageResult receiveMessageResult;
        while (true) {
            while (sqs.getQueueAttributes(getQueueAttributesRequest).getAttributes().isEmpty()) {
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            receiveMessageResult = sqs.receiveMessage(downstreamURL);
            if (receiveMessageResult.toString().contains(id)) {
                List<Message> messages = receiveMessageResult.getMessages();
                for (Message message : messages) {
                    if (message.toString().contains(id)) {
                        String messageReceiptHandle = message.getReceiptHandle();
                        sqs.deleteMessage(new DeleteMessageRequest(downstreamURL, messageReceiptHandle));
                        break;
                    }
                }
                break;
            }
        }
        System.out.print("Downloading results file from S3... ");
        S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, id + RESULTS_FILE_SUFFIX));
        System.out.println("Done.");
        File output = new File(System.getProperty("user.dir") + "/" + outputFileName);
        FileWriter fw = new FileWriter(output);
        fw.write(HEADER);
        Scanner scanner = new Scanner(object.getObjectContent());
        scanner.useDelimiter("<delimiter>");
        //tokenize the reservations file and process each token one at a time
        while (scanner.hasNext()) {
            String result = scanner.next();
            String tweet = result.substring(result.indexOf("<tweet>") + 7, result.indexOf("</tweet>"));
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
    }

    /**
     * Displays the contents of the specified input stream as text.
     *
     * @param input
     *            The input stream to display as text.
     *
     * @throws IOException
     */
    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

    private static void deployManager() {
//        AmazonEC2 ec2;
//        AWSCredentials credentials = null;
//        try {
//            credentials = new PropertiesCredentials(Local.class.getResourceAsStream("~/.aws/credentials"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        ec2 = new AmazonEC2Client(credentials);
//        CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest();
//        createKeyPairRequest.withKeyName("test");
//        CreateKeyPairResult createKeyPairResult = ec2.createKeyPair(createKeyPairRequest);
//        KeyPair keyPair = new KeyPair();
//        keyPair = createKeyPairResult.getKeyPair();
//        String privateKey = keyPair.getKeyMaterial();
//        System.out.println(privateKey);
//        try {
//            RunInstancesRequest request = new RunInstancesRequest("ami-b66ed3de", 1, 1);
//            request.setInstanceType(InstanceType.T2Micro.toString());
//            List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
//            System.out.println("Launch instances: " + instances);
//
//        } catch (AmazonServiceException ase) {
//            System.out.println("Caught Exception: " + ase.getMessage());
//            System.out.println("Reponse Status Code: " + ase.getStatusCode());
//            System.out.println("Error Code: " + ase.getErrorCode());
//            System.out.println("Request ID: " + ase.getRequestId());
//        }
    }

}