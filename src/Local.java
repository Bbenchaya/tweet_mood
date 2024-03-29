/**
 * Created by asafchelouche on 25/3/16.
 *
 * The local client.
 *
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
    private static final String HEADER =
            "<!DOCTYPE html>" +
            "\n\t<html>" +
                "\n\t<head>" +
                    "\n\t\t<title>DSP 162, assignment 1</title>" +
                    "\n\t\t<link type=\"text/css\" rel=\"stylesheet\" href=\"stylesheet.css\"/>" +
                "\n\t</head>" +
                "\n\t<body>" +
                    "\n\t\t<div class=\"document\">" +
                    "\n\t\t<div><h1 align=\"center\">DSP 161, Assignment 1</h1></div>" +
                    "\n\t\t<div align=\"center\"><img src=\"rubyrhod.jpeg\" /></div>";
    private static final String FOOTER =
                "\n\t</body>\n" +
                "</html>";
    private static boolean managerShouldTerminate;
    private static String upstreamURL;
    private static String downstreamURL;
    private static final String URLS_FILENAME = "up-down.txt";
    private static AmazonS3 s3;
    private static AmazonSQS sqs;
    private static AmazonEC2 ec2;

    /**
     * Main method.
     * @param args command line arguments - required structure is described in README.md
     * @throws IOException
     */
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

    /**
     * Alert the Manager instance of a new request in the UPSTREAM queue.
     */
    private static void sendRequestInUpstream() {
        sqs.sendMessage(upstreamURL, id + LINKS_FILENAME_SUFFIX);
    }

    /**
     * Awaits for the results message to appear in the DOWNSTREAM queue, downloads the results file from S3 and compiles
     * it to an HTML file, whose name is as supplied in the command line during execution.
     * @throws IOException
     */
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
//        Scanner scanner = new Scanner(new File("results.txt"));
        scanner.useDelimiter("<delimiter>");
        int[] sentimentCounters = new int[5];
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
            String cssColorClass = null;
            switch(sentiment) {
                case "0":
                    cssColorClass = "s0"; //dark red
                    sentimentCounters[0]++;
                    break;
                case "1":
                    cssColorClass = "s1";
                    sentimentCounters[1]++;
                    break;
                case "2":
                    cssColorClass = "s2";
                    sentimentCounters[2]++;
                    break;
                case "3":
                    cssColorClass = "s3";
                    sentimentCounters[3]++;
                    break;
                case "4":
                    cssColorClass = "s4";
                    sentimentCounters[4]++;
                    break;
            }
            fw.write("\n\t\t");
            fw.write("<div class=\"" + cssColorClass + "\"><p><b>");
            fw.write(generateHashtags(generateHandles(generateLinks(tweet))));
            fw.write("</b><br>");
            fw.write(entities);
            fw.write("</p></div>");
            fw.flush();
        }
        fw.write(FOOTER);
        fw.flush();
        fw.close();
        scanner.close();
        if (managerShouldTerminate) {
            sqs.sendMessage(upstreamURL, "terminate");
            System.out.println("Sent termination message to the Manager.");
        }
        System.out.println("Finished execution, exiting...");
    }

    private static String generateHashtags(String tweet) {
        String prefix = "<a href=\"http://twitter.com/hashtag/";
        String prefix2 = "?src=hash\">";
        String suffix = "</a>";
        List<String> hashtags = new LinkedList<>();
        int end = 0;
        int start;
        while ((start = tweet.indexOf("#", end)) != -1) {
            end = linkEnd(tweet, start);
            hashtags.add(tweet.substring(start, end));
        }
        for (String hashtag : hashtags)
            tweet = tweet.replace(hashtag, prefix + hashtag.substring(1, hashtag.length()) + prefix2 + hashtag + suffix);
        return tweet;
    }

    private static String generateHandles(String tweet) {
        String prefix = "<a href=\"http://twitter.com/";
        String prefix2 = "\">";
        String suffix = "</a>";
        List<String> handles = new LinkedList<>();
        int end = 0;
        int start;
        while ((start = tweet.indexOf("@", end)) != -1) {
            end = handleEnd(tweet, start);
            handles.add(tweet.substring(start, end));
        }
        for (String handle : handles)
            tweet = tweet.replace(handle, prefix + handle.substring(1, handle.length()) + prefix2 + handle + suffix);
        return tweet;
    }

    private static int handleEnd(String tweet, int from) {
        int length = tweet.length();
        from++;
        while (from < length) {
            char c = tweet.charAt(from);
            if (!(('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || (c == '_')))
                return from;
            from++;
        }
        return tweet.length();
    }

    private static int linkEnd(String tweet, int from) {
        int length = tweet.length();
        while (from < length) {
            switch (tweet.charAt(from)) {
                case ' ':
                case '\"':
                    return from;
            }
            from++;
        }
        return tweet.length();
    }

    private static String generateLinks(String tweet) {
        String prefix = "<a href=\"";
        String prefix2 = "\">";
        String suffix = "</a>";
        List<String> links = new LinkedList<>();
        int end = 0;
        int start;
        while ((start = tweet.indexOf("http", end)) != -1) {
            end = linkEnd(tweet, start);
            links.add(tweet.substring(start, end));
        }
        for (String link : links)
            tweet = tweet.replace(link, prefix + link + prefix2 + link + suffix);
        return tweet;
    }

    /**
     * A generator for user data to be attached to the EC2 instance reservation.
     * @return the user data, encoded in base-64.
     */
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