import com.amazonaws.AmazonServiceException;
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
    private static AtomicInteger pendingTweets;
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

        init();

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
        System.out.print("Uploading the worker queues' URLs file to S3... ");
        s3.putObject(new PutObjectRequest(BUCKET_NAME, WORKER_QUEUES_FILENAME, file));
        System.out.println("Done.");

        // create and run a thread that polls the UPSTREAM queue for work requests and the termination message
        Thread awaitAndProcessRequests = new Thread() {
            @Override
            public void run() {
                GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(upstreamURL);
                List<String> attributeNames = new LinkedList<>();
                attributeNames.add("ApproximateNumberOfMessages");
                getQueueAttributesRequest.setAttributeNames(attributeNames);
                ReceiveMessageResult receiveMessageResult;
                System.out.println("Awaiting incoming requests...");
                while (Manager.shouldProcessRequests()) {
                    String id = null;
                    while (sqs.getQueueAttributes(getQueueAttributesRequest).getAttributes().isEmpty()) {
                        try {
                            sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println("Receiving incoming request at UPSTREAM...");
                    receiveMessageResult = sqs.receiveMessage(upstreamURL);
                    if (!receiveMessageResult.toString().contains("terminate")) { // there's no termination message in the UPSTREAM queue
                        List<Message> messages = receiveMessageResult.getMessages();
                        // TODO check if necessary, maybe switch to messages.getMessages().get(0)
                        for (Message message : messages) {
                            if (message.getBody().contains("links.txt")) {
                                id = message.getBody().substring(0, message.getBody().indexOf("links.txt"));
                                // create an entry that will hold the results of the request
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
                                System.out.println("Handling the request...");
                                //download the tweets themselves and parse them
                                Stack<String> links = new Stack<>();
                                String link;
                                int numOfTweets = 0;
                                while ((link = scanner.nextLine()) != null) {
                                    links.push(link);
                                    numOfTweets++;
                                    if (((double) numOfTweets) / workersPerTweetsRatio - numOfActiveWorkers.get() >= 0) {
                                        System.out.println("Creating new worker. Number of pending tweets: " + pendingTweets.get());
                                        // at this point, there's already in entry in 'requests' for the relevant request
                                        Manager.createWorker();
                                    }
                                }
                                scanner.close();
                                // TODO synchronize?
                                requests.get(finalId).setNumOfExpectedResults(numOfTweets);
                                Document doc = null;
                                Elements content;
                                System.out.print("Parsing " + numOfTweets + " tweets... ");
                                for (String link2 : links) {
                                    try {
                                        doc = Jsoup.connect(link2).get();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                    content = doc.select("title");
                                    sqs.sendMessage(jobsURL, "<id>" + finalId + "</id><content>" + content.text() + "</content>");
                                    pendingTweets.addAndGet(1);
                                }
                                System.out.println("Done.");
                            }
                        };
                        getAndParseTweetLinksFile.start();
                    }
                    else {  // termination message received in UPSTREAM queue
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
                    // TODO synchronize?
                    Iterator<Map.Entry<String, RequestStatus>> it = requests.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<String, RequestStatus> keyValue = it.next();
                        if (keyValue.getValue().hasAllResults()) {
                            System.out.println("Compiling results");
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

        // setup and run the thread that awaits and processes results
        Thread findAndHandleResults = new Thread() {
            @Override
            public void run() {
                while (Manager.isAlive()) {
                    ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(resultsURL);
                    for (Message message : receiveMessageResult.getMessages()) {
                        String body = message.getBody();
                        String id = body.substring(body.indexOf("<id>") + 4, body.indexOf("</id>"));
                        String result = body.substring(body.indexOf("<tweet>"), body.length());
                        // TODO synchronize?
                        requests.get(id).addResult(result);
                        pendingTweets.addAndGet(-1);
                        String messageReceiptHandle = message.getReceiptHandle();
                        sqs.deleteMessage(new DeleteMessageRequest(upstreamURL, messageReceiptHandle));
                        System.out.println("Processed result: id: " + id + " result: " + result);
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

        // TODO add a thread that checks if all workers are dead but no termination message was received, so start new Worker instances

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
            // if there are no running workers, break and finish execution
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

    private static void init() {
        requests = new ConcurrentHashMap<>();
        alive = new AtomicBoolean();
        alive.set(true);
        shouldProcessRequests = new AtomicBoolean();
        shouldProcessRequests.set(true);
        numOfActiveWorkers = new AtomicInteger();
        numOfActiveWorkers.set(-1);
        pendingTweets = new AtomicInteger();
        pendingTweets.set(0);

        // initiate connection to S3
        s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        System.out.println("Manager running...");

        // initiate connection to EC2
        ec2 = new AmazonEC2Client();
        ec2.setRegion(usEast1);

        // initiate connection to SQS
        sqs = new AmazonSQSClient();
        sqs.setRegion(usEast1);
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
            RunInstancesRequest request = new RunInstancesRequest("ami-4504112f", 1, 1); // base AMI: b66ed3de
            request.setInstanceType(InstanceType.T2Micro.toString());
            request.setUserData(getUserDataScript());
            IamInstanceProfileSpecification iamInstanceProfileSpecification = new IamInstanceProfileSpecification();
            iamInstanceProfileSpecification.setName("creds");
            request.setIamInstanceProfile(iamInstanceProfileSpecification);
            List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
            System.out.println("Launch instances: " + instances);
            CreateTagsRequest createTagRequest = new CreateTagsRequest();
            createTagRequest.withResources(instances.get(0).getInstanceId()).withTags(new Tag("kind", "worker"));
            ec2.createTags(createTagRequest);
            // the synchronization looks redundant, but using atomics makes the code easier in 2 other places
            synchronized (numOfActiveWorkers) {
                if (numOfActiveWorkers.get() == -1)
                    numOfActiveWorkers.set(1);
                else
                    numOfActiveWorkers.addAndGet(1);
            }
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Response Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }

    private static String getUserDataScript(){
        StringBuilder sb = new StringBuilder();
        sb.append("#! /bin/bash\n");
        sb.append("aws s3 cp s3://asafbendsp/ejml-0.23.jar ejml-0.23.jar\n");
        sb.append("aws s3 cp s3://asafbendsp/jollyday-0.4.7.jar jollyday-0.4.7.jar\n");
        sb.append("aws s3 cp s3://asafbendsp/stanford-corenlp-3.3.0-models.jar stanford-corenlp-3.3.0-models.jar\n");
        sb.append("aws s3 cp s3://asafbendsp/stanford-corenlp-3.3.0.jar stanford-corenlp-3.3.0.jar\n");
        sb.append("aws s3 cp s3://asafbendsp/Worker.jar Worker.jar\n");
        sb.append("wget --no-check-certificate --no-cookies --header \"Cookie: oraclelicense=accept-securebackup-cookie\" http://download.oracle.com/otn-pub/java/jdk/8u73-b02/jdk-8u73-linux-x64.rpm\n");
        sb.append("sudo rpm -i jdk-8u73-linux-x64.rpm");
        sb.append("jar xf Worker.jar\n");
        sb.append("java -cp .:./* Worker\n");
        // AWS requires that user data be encoded in base-64
        String str = new String(Base64.encodeBase64(sb.toString().getBytes()));
        return str;
    }

}