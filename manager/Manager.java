import java.io.*;
import java.util.*;

import com.amazonaws.services.ec2.AmazonEC2;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.*;
import java.util.List;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

public class Manager {

    private static final String bucket = "asafbendsp";
    private static final String inputFileName = "/Users/bbenchaya/Documents/Programming/DSP/tweet_mood/src/short.txt";
//    private static final String inputFileName = "/Users/asafchelouche/programming/tweet_mood/src/short.txt";
    private static final String objectName = "tweetLinks.txt";
    private static final String BUCKET_NAME = "asafbendsp";
    private static final String URLS_FILENAME = "up-down.txt";
    private static final String WORKER_QUEUES_FILENAME = "jobs-results.txt";
    private static double jobsPerWorker;
    private static String upstreamURL;
    private static String downstreamURL;
    private static String jobs;
    private static String results;
    private static List<Worker> workers;
    private static AtomicBoolean isAlive;
    private static AmazonEC2 ec2;
    private static AmazonS3 s3;
    private static AmazonSQS sqs;
    private static List<String> ids;
    private static ConcurrentHashMap<String, Container> requests;

    private static boolean isAlive() {
        return isAlive.get();
    }

    public static void main(String[] args) throws IOException {

        ids = new LinkedList<>();
        requests = new ConcurrentHashMap<>();
        isAlive.set(true);

        // initiate connection to S3
        s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        System.out.println("Manager running...");
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

        /*
        Setup and run the thread that polls for completed request, and then compile the results for the DOWNSTREAM.
         */
        Thread resultsPoller = new Thread() {
            @Override
            public void run() {
                while (Manager.isAlive()) {
                    for (Container container : requests.values()) {
                        if (container.hasAllResults()) {
                            // TODO compile the results to UUIDresults.txt, save it to S3, and place its name in DOWNSTREAM
                        }
                    }
                }
            }
        };
        resultsPoller.start();

        // initiate connection to EC2
        ec2 = new AmazonEC2Client(credentials);
        ec2.setRegion(usEast1);

        // initiate connection to SQS
        sqs = new AmazonSQSClient(credentials);
        sqs.setRegion(usEast1);

        // get the  SQS URLs file from S3
        System.out.print("Downloading URLs file from S3... ");
        S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, URLS_FILENAME));
        System.out.println("Done.");
        BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
        upstreamURL = br.readLine();
        downstreamURL = br.readLine();
        br.close();

        // create the SQSs to interface with the workers
        System.out.print("Creating jobs queue...");
        CreateQueueRequest createJobsQueueRequest = new CreateQueueRequest("jobs");
        jobs = sqs.createQueue(createJobsQueueRequest).getQueueUrl();
        System.out.println("Done.");
        System.out.print("Creating results queue...");
        CreateQueueRequest createResultsQueueRequest = new CreateQueueRequest("results");
        results = sqs.createQueue(createResultsQueueRequest).getQueueUrl();
        System.out.println("Done.");

        // create and run a thread that polls the UPSTREAM queue for work requests and the termination message
        Thread workRequests = new Thread() {
            @Override
            public void run() {
                GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(downstreamURL);
                ReceiveMessageResult receiveMessageResult;
                while (Manager.isAlive()) {
                    String id = null;
                    while (sqs.getQueueAttributes(getQueueAttributesRequest).getAttributes().isEmpty()) {
                        try {
                            sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    receiveMessageResult = sqs.receiveMessage(upstreamURL);
                    if (receiveMessageResult.toString().contains("links.txt")) {
                        List<Message> messages = receiveMessageResult.getMessages();
                        for (Message message : messages) {
                            if (message.toString().contains("links.txt")) {
                                id = message.toString().substring(0, message.toString().indexOf("links.txt"));
                                ids.add(id);
                                requests.put(id, new Container());
                                String messageReceiptHandle = message.getReceiptHandle();
                                sqs.deleteMessage(new DeleteMessageRequest(upstreamURL, messageReceiptHandle));
                                break;
                            }
                        }

                        // download the tweet links file from S3
                        System.out.print("Downloading tweet links file from S3... ");
                        S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, id + "links.txt"));
                        System.out.println("Done.");
                        Scanner scanner = new Scanner(new InputStreamReader(object.getObjectContent()));
                        final String finalId = id;
                        Thread getAndParse = new Thread() {
                            @Override
                            public void run() {
                                //download the tweets themselves and parse them
                                Stack<String> links = new Stack<>();
                                String line;
                                int numOfTweets = 0;
                                while ((line = scanner.nextLine()) != null) {
                                    links.push(line);
                                    numOfTweets++;
                                }
                                scanner.close();
                                requests.get(finalId).setNumOfExpectedResults(numOfTweets);

                                // TODO extract to Worker
//                                Document doc = null;
//                                Elements content;
//                                System.out.print("Parsing tweets... ");
//                                for (String link : links) {
//                                    try {
//                                        doc = Jsoup.connect(link).get();
//                                    } catch (IOException e) {
//                                        e.printStackTrace();
//                                    }
//                                    content = doc.select("title");
//                                    // TODO
//                                    sqs.sendMessage(jobs, ""content.text());
//                                }
//                                System.out.println("Done.");

                                //determine the number of workers to initiate
//                                workers = new LinkedList<>();
//                                int numOfWorkers = (int) Math.ceil(jobs.queue.size() / jobsPerWorker);
//                                System.out.println("Creating workers and sending them to work...");
//                                for (int i = 0; i < numOfWorkers; i++) {
//                                    Worker worker = new Worker(jobs, results);
//                                    workers.add(worker);
//                                    worker.work();
//                                }
                            }
                        };
                        getAndParse.start();
//                        break;
                    }
                }
            }
        };
        workRequests.start();
    }

}