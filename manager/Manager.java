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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

// TODO extract the different code sections to methods

public class Manager {

    private static final String BUCKET_NAME = "asafbendsp";
    private static final String URLS_FILENAME = "up-down.txt";
    private static final String WORKER_QUEUES_FILENAME = "jobs-results.txt";
    private static final String OUTPUT_FILENAME_SUFFIX = "output.txt";
    private static int workersPerTweetsRatio;
    private static AtomicInteger numOfActiveWorkers;
    private static String upstreamURL;
    private static String downstreamURL;
    private static String jobsURL;
    private static String resultsURL;
    private static AtomicBoolean alive;
    private static AtomicBoolean shouldProcessRequests;
    private static AmazonEC2 ec2;
    private static AmazonS3 s3;
    private static AmazonSQS sqs;
    private static ConcurrentHashMap<String, RequestStatus> requests;

    private static boolean isAlive() {
        return alive.get();
    }

    public static void main(String[] args) throws IOException {

        requests = new ConcurrentHashMap<>();
        alive.set(true);
        shouldProcessRequests.set(true);
        numOfActiveWorkers.set(-1);

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
        workersPerTweetsRatio = Integer.parseInt(br.readLine());
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
        Thread awaitAndProcessRequests = new Thread() {
            @Override
            public void run() {
                GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(downstreamURL);
                ReceiveMessageResult receiveMessageResult;
                while (Manager.shouldProcessRequests()) {
                    String id = null;
                    while (sqs.getQueueAttributes(getQueueAttributesRequest).getAttributes().isEmpty()) {
                        try {
                            sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    receiveMessageResult = sqs.receiveMessage(upstreamURL);
                    if (!receiveMessageResult.toString().contains("terminate")) {
                        List<Message> messages = receiveMessageResult.getMessages();
                        // TODO check if necessary, maybe switch to messages.getMessages().get(0)
                        for (Message message : messages) {
                            if (message.toString().contains("links.txt")) {
                                id = message.toString().substring(0, message.toString().indexOf("links.txt"));
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
                                    if (((double) numOfTweets) / workersPerTweetsRatio - numOfActiveWorkers.get() >= 0) {
                                        Manager.createWorker();
                                    }
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
                            }
                        };
                        getAndParseTweetLinksFile.start();
                    }
                    else {
                        shouldProcessRequests.set(false);
                    }
                }
            }
        };
        awaitAndProcessRequests.start();

        /*
        Setup and run the thread that polls for completed requests, and then compile the results and place in DOWNSTREAM
         */
        Thread compileResults = new Thread() {
            @Override
            public void run() {
                while (Manager.isAlive()) {
                    Iterator<Map.Entry<String, RequestStatus>> it = requests.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<String, RequestStatus> keyValue = it.next();
                        if (keyValue.getValue().hasAllResults()) {
                            Manager.compileAndSendResults(keyValue.getKey(), keyValue.getValue().getResults());
                            requests.remove(keyValue.getKey());
                        }
                    }
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        compileResults.start();

        Thread findAndHandleResults = new Thread() {
            @Override
            public void run() {
                while (Manager.isAlive()) {
                    ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(resultsURL);
                    for (Message message : receiveMessageResult.getMessages()) {
                        String body = message.getBody();
                        String id = body.substring(body.indexOf("<id>") + 4, body.indexOf("</id>"));
                        String result = body.substring(body.indexOf("<tweet>") + 7, body.length());
                        requests.get(id).addResult(result);
                    }
                    try {
                        sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        findAndHandleResults.start();

        // wait for all Workers to DIE!!!
        List<String> tagValues = new ArrayList<>();
        tagValues.add("worker");
        Filter tagFilter = new Filter("tag:kind", tagValues);
        List<String> statusValues = new ArrayList<>();
        statusValues.add("running");
        Filter statusFilter = new Filter("instance-state-name", statusValues);
        while (true) {
            DescribeInstancesResult filteredInstances = ec2.describeInstances(new DescribeInstancesRequest().withFilters(tagFilter, statusFilter));
            List<Reservation> reservations = filteredInstances.getReservations();
            if (reservations.size() == 0 && numOfActiveWorkers.get() >= 0)
                break;
            else
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }

        // Manager has ended execution

    }

    private static boolean shouldProcessRequests() {
        return shouldProcessRequests.get();
    }

    private static void compileAndSendResults(String key, String results) {
        String output = "<id>" + key + "</id>" + results;
        File file = new File(key + OUTPUT_FILENAME_SUFFIX);
        FileWriter fw = null;
        try {
            fw = new FileWriter(file);
            fw.write(output);
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.print("Finished compiling results file, uploading to S3... ");
        s3.putObject(new PutObjectRequest(BUCKET_NAME, key + OUTPUT_FILENAME_SUFFIX, file));
        System.out.println("Done.");
        sqs.sendMessage(downstreamURL, key + "done");
    }

    private static void createWorker() {
        // start a Worker instance
        try {
            RunInstancesRequest request = new RunInstancesRequest("ami-b66ed3de", 1, 1);
            request.setInstanceType(InstanceType.T2Micro.toString());
            List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
            System.out.println("Launch instances: " + instances);
            CreateTagsRequest createTagRequest = new CreateTagsRequest();
            createTagRequest.withResources(instances.get(0).getInstanceId()).withTags(new Tag("kind", "worker"));
            ec2.createTags(createTagRequest);
            if (numOfActiveWorkers.get() == -1)
                numOfActiveWorkers.set(1);
            else
                numOfActiveWorkers.addAndGet(1);
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Response Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }

}