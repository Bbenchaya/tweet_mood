import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO extract the different code sections to methods

public class Manager {

    private static final String BUCKET_NAME = "asafbendsp";
    private static final String URLS_FILENAME = "up-down.txt";
    private static final String WORKER_QUEUES_FILENAME = "jobs-results.txt";
    // TODO handle this shit
    private static int workersToFileRatio;
    private static String upstreamURL;
    private static String downstreamURL;
    private static String jobsURL;
    private static String resultsURL;
    private static List<Worker> workers;
    private static AtomicBoolean isAlive;
    private static AmazonEC2 ec2;
    private static AmazonS3 s3;
    private static AmazonSQS sqs;
    private static List<String> ids;
    private static ConcurrentHashMap<String, RequestStatus> requests;

    private static boolean isAlive() {
        return isAlive.get();
    }

    public static void main(String[] args) throws IOException {

        ids = new LinkedList<>(); // TODO should we remove this?
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
        Setup and run the thread that polls for completed requests, and then compile the results and place in DOWNSTREAM
         */
        Thread compileResults = new Thread() {
            @Override
            public void run() {
                while (Manager.isAlive()) {
                    for (RequestStatus requestStatus : requests.values()) {
                        if (requestStatus.hasAllResults()) {
                            // TODO compile the resultsURL to UUIDresults.txt, save it to S3, and place its name in DOWNSTREAM
                        }
                    }
                }
            }
        };
        compileResults.start();

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
        workersToFileRatio = Integer.parseInt(br.readLine());
        br.close();

        // create the queues to interface with the workers
        System.out.print("Creating jobs queue...");
        CreateQueueRequest createJobsQueueRequest = new CreateQueueRequest("jobs");
        jobsURL = sqs.createQueue(createJobsQueueRequest).getQueueUrl();
        System.out.println("Done.");
        System.out.print("Creating results queue...");
        CreateQueueRequest createResultsQueueRequest = new CreateQueueRequest("results");
        resultsURL = sqs.createQueue(createResultsQueueRequest).getQueueUrl();
        System.out.println("Done.");

        // create a file that holds the queues' URLs, and upload it to S3 for the workers
        File file = new File(System.getProperty("user.dir") + "/" + WORKER_QUEUES_FILENAME);
        FileWriter fw = new FileWriter(file);
        fw.write(jobsURL + "\n");
        fw.write(resultsURL + "\n");
        fw.close();
        System.out.print("Uploading the URLs file to S3... ");
        s3.putObject(new PutObjectRequest(BUCKET_NAME, WORKER_QUEUES_FILENAME, file));
        System.out.println("Done.");

        // create and run a thread that polls the UPSTREAM queue for work requests and the termination message
        Thread awaitRequests = new Thread() {
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

                    // TODO seret????
                    receiveMessageResult = sqs.receiveMessage(upstreamURL);
                    if (receiveMessageResult.toString().contains("links.txt")) {
                        List<Message> messages = receiveMessageResult.getMessages();
                        // TODO check if necessary, maybe switch to messages.getMessages().get(0)
                        for (Message message : messages) {
                            if (message.toString().contains("links.txt")) {
                                id = message.toString().substring(0, message.toString().indexOf("links.txt"));
                                ids.add(id); // TODO remove?
                                requests.put(id, new RequestStatus());
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
                        Thread getAndParseTweetLinksFile = new Thread() {
                            @Override
                            public void run() {

                                //download the tweets themselves and parse them
                                Stack<String> links = new Stack<>();
                                String link;
                                int numOfTweets = 0;
                                while ((link = scanner.nextLine()) != null) {
                                    links.push(link);
                                    numOfTweets++;
                                }
                                scanner.close();
                                requests.get(finalId).setNumOfExpectedResults(numOfTweets);
                                Document doc = null;
                                Elements content;
                                System.out.print("Parsing tweets... ");
                                for (String link2 : links) {
                                    try {
                                        doc = Jsoup.connect(link2).get();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                    content = doc.select("title");
                                    sqs.sendMessage(jobsURL, "<id>" + finalId + "</id><content>" + content.text() + "</content>");
                                }
                                System.out.println("Done.");

                                // TODO determine the number of workers to initiate
//                                workers = new LinkedList<>();
//                                int numOfWorkers = (int) Math.ceil(jobsURL.queue.size() / jobsPerWorker);
//                                System.out.println("Creating workers and sending them to work...");
//                                for (int i = 0; i < numOfWorkers; i++) {
//                                    Worker worker = new Worker(jobsURL, resultsURL);
//                                    workers.add(worker);
//                                    worker.work();
//                                }
                            }
                        };
                        getAndParseTweetLinksFile.start();
//                        break; // TODO remove?
                    }
                    else {
                        // TODO handle termination message
                    }
                }
            }
        };
        awaitRequests.start();

    }

}