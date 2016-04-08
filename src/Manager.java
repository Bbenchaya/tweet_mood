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

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

public class Manager {

    private static final String BUCKET_NAME = "asafbendsp";
    private static final String URLS_FILENAME = "up-down.txt";
    private static final String WORKER_QUEUES_FILENAME = "jobs-results.txt";
    private static final String RESULTS_FILENAME_SUFFIX = "results.txt";
    private static final String WORKER_STATS_FILENAME = "worker-statistics.txt";
    private static final int THREAD_POOL_SIZE = 10;
    private static int workersPerTweetsRatio;
    private static AtomicInteger numOfActiveWorkers;
    private static AtomicInteger pendingTweets;
    private static boolean firstRequestReceived;
    private static String upstreamURL;
    private static String downstreamURL;
    private static String jobsURL;
    private static String resultsURL;
    private static AtomicBoolean shouldTerminate;
    private static AtomicBoolean shouldProcessRequests;
    private static AtomicBoolean firstWorkerRunning;
    private static AmazonEC2 ec2;
    private static AmazonS3 s3;
    private static AmazonSQS sqs;
    private static ConcurrentHashMap<String, RequestStatus> requests;
    private static HashMap<String, WorkerStatistics> workerStatistics;
    private static ExecutorService pool;

    private static boolean shouldTerminate() {
        return shouldTerminate.get();
    }

    /**
     * Main method
     * @param args command line arguments - completely discarded
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException {

        init();

        // get the  SQS URLs file from S3
        System.out.print("Downloading upstream\\downstream queues' URLs file from S3... ");
        S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, URLS_FILENAME));
        System.out.println("Done.");
        BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
        upstreamURL = br.readLine();
        downstreamURL = br.readLine();
        workersPerTweetsRatio = Integer.parseInt(br.readLine());
        br.close();

        // create the queues to interface with the workers
        System.out.print("Creating jobs queue... ");
        CreateQueueRequest createJobsQueueRequest = new CreateQueueRequest("jobs");
        jobsURL = sqs.createQueue(createJobsQueueRequest).getQueueUrl();
        System.out.println("Done.");
        System.out.print("Creating results queue... ");
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
                    while ((receiveMessageResult = sqs.receiveMessage(upstreamURL)).getMessages().isEmpty()) {
                        try {
                            sleep(250);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    if (!receiveMessageResult.toString().contains("terminate")) { // there's no termination message in the UPSTREAM queue
                        List<Message> messages = receiveMessageResult.getMessages();
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

                        if (id == null)
                            continue;
                        // download the tweet links file from S3
                        System.out.format("Downloading tweet links file from S3: %s... ", id + "links.txt");
                        S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, id + "links.txt"));
                        System.out.println("Done.");
                        final BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
                        final String finalId = id;

                        Thread getAndParseTweetLinksFile = new Thread() {
                            @Override
                            public void run() {
                                System.out.println("Handling request for Local id: " + finalId);
                                // count the number of links in the file in order to determinte the number of new workers that need to be raised
                                List<String> links = new LinkedList<>();
                                String link = null;
                                int numOfTweets = 0;
                                try {
                                    while ((link = br.readLine()) != null) {
                                        numOfTweets++;
                                        links.add(link);
                                    }
                                } catch (IOException e) {
                                    e.printStackTrace();
                                    System.out.println(Thread.currentThread().getName());
                                }
                                try {
                                    br.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }

                                // create new Worker instances, if required
                                int numOfNewWorkersToRaise =  (numOfTweets / workersPerTweetsRatio) - numOfActiveWorkers.get();
                                for (int i = 0; i < numOfNewWorkersToRaise; i++) {
                                    Manager.createWorker();
                                }

                                // parse the links from the file and create new jobs for the Workers
                                for (String link2 : links) {
                                    sqs.sendMessage(jobsURL, "<id>" + finalId + "</id><link>" + link2 + "</link>");
                                    pendingTweets.incrementAndGet();
                                }
                                requests.get(finalId).setNumOfExpectedResults(numOfTweets);
                                firstRequestReceived = true;
                                System.out.println("Finished distributing jobs in response to request from Local id: " + finalId);
                            }
                        };
                        pool.submit(getAndParseTweetLinksFile);
                    }
                    else {  // termination message received in UPSTREAM queue
                        shouldProcessRequests.set(false);
                        sqs.sendMessage(jobsURL, "terminate");
                        System.out.println("Termination message received from Local id: " + id);
                        System.out.println("Incoming requests will be ignored from now, Manager is still waiting for results for previous requests.");
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
                while (!Manager.shouldTerminate()) {
                    // NOTE: access to a concurrent hashmap via entry set is thread-safe
                    for (Map.Entry<String, RequestStatus> keyValue : requests.entrySet()) {
                        if (keyValue.getValue().hasAllResults()) {
                            System.out.println("Compiling results for Local id: " + keyValue.getKey());
                            Manager.compileAndSendResults(keyValue.getKey(), keyValue.getValue().getResults());
                            requests.remove(keyValue.getKey());
                            System.out.println("Finished compiling results for Local id: " + keyValue.getKey());
                        }
                    }
                    try {
                        sleep(250);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("All requests have been processed, and all result files have been uploaded to S3.");
            }
        };
        compileResults.start();

        Thread raiseRedundantWorkers = new Thread() {
            @Override
            public void run() {
                while (!firstWorkerRunning.get()) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                List<String> tagValues = new ArrayList<>();
                tagValues.add("worker");
                Filter tagFilter = new Filter("tag:kind", tagValues);
                List<String> statusValues = new ArrayList<>();
                statusValues.add("running");
                statusValues.add("pending");
                Filter statusFilter = new Filter("instance-state-name", statusValues);
                List<Reservation> reservations;
                DescribeInstancesResult filteredInstances;
                DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withFilters(tagFilter, statusFilter);
                while (!shouldTerminate()) {
                    filteredInstances = ec2.describeInstances(describeInstancesRequest);
                    reservations = filteredInstances.getReservations();
                    if (reservations.isEmpty() && pendingTweets.get() > 0) {
                        int numOfMissingWorkers = pendingTweets.get() / workersPerTweetsRatio + 1;
                        System.out.println("Worker instances have unexpectedly stopped. No. of missing workers: " + numOfMissingWorkers);
                        System.out.println("Launching replacement instances...");
                        for (int i = 0; i < numOfMissingWorkers && firstWorkerRunning.get(); i++) {
                            Manager.createWorker();
                        }
                        try {
                            sleep(5000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        System.out.println("Finished launching replacement instances.");
                    }
                    try {
                        sleep(30000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        raiseRedundantWorkers.start();

        // await and processes results
        while (!(firstRequestReceived && requests.isEmpty() && pendingTweets.get() == 0 && !Manager.shouldTerminate())) {
            ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(resultsURL);
            for (Message message : receiveMessageResult.getMessages()) {
                String body = message.getBody();
                String workerId = body.substring(body.indexOf("<worker-id>") + 11, body.indexOf("</worker-id>"));
                workerStatistics.putIfAbsent(workerId, new WorkerStatistics());
                if (body.contains("<dropped-link>")) {
                    System.out.println("dropped link");
                    String requestId = body.substring(body.indexOf("<local-id>") + 10, body.indexOf("</local-id>"));
                    requests.get(requestId).decrementExpectedResults();
                    String messageReceiptHandle = message.getReceiptHandle();
                    sqs.deleteMessage(new DeleteMessageRequest(resultsURL, messageReceiptHandle));
                    pendingTweets.decrementAndGet();
                    workerStatistics.get(workerId).addDropped();
                }
                else {
                    String id = body.substring(body.indexOf("<id>") + 4, body.indexOf("</id>"));
                    String result = body.substring(body.indexOf("<tweet>"), body.length());
                    RequestStatus requestStatus = requests.get(id);
                    if (requestStatus != null)
                        requests.get(id).addResult(result);
                    String messageReceiptHandle = message.getReceiptHandle();
                    sqs.deleteMessage(new DeleteMessageRequest(resultsURL, messageReceiptHandle));
//                    System.out.println("Processed result: id: " + id + " result: " + result);
                    pendingTweets.decrementAndGet();
                    workerStatistics.get(workerId).addSuccessful();
                }
            }
            try {
                sleep(250);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        shouldTerminate.set(true);

        compileWorkerStatistics();

        pool.shutdownNow();
        System.out.println("Manager has finished execution, exiting...");
        Runtime rt = Runtime.getRuntime();
        Process pr = rt.exec("shutdown -h now"); // sends a kill message to the EC2 instance

    }

    /**
     * Compiles the statistics file for all Worker instances, and uploads it to S3.
     * @throws IOException
     */
    private static void compileWorkerStatistics() throws IOException {
        System.out.print("Compiling worker statistics... ");
        File statsFile = new File(WORKER_STATS_FILENAME);
        FileWriter fw = new FileWriter(statsFile);
        for (Map.Entry<String, WorkerStatistics> stats : workerStatistics.entrySet()) {
            int successful = stats.getValue().getSuccessful();
            int dropped = stats.getValue().getDropped();
            String workerId = stats.getKey();
            fw.write("Worker - \n");
            fw.write("\t\t\tid: " + workerId + "\n");
            fw.write("\t\t\ttotal links processed: " + (successful + dropped) + "\n");
            fw.write("\t\t\tdropped links: " + dropped + "\n");
            fw.write("\t\t\tsuccessful links: " + successful + "\n\n\n");
            fw.flush();
        }
        fw.close();
        System.out.println("Finished compiling worker statistics summary.");
        System.out.println("Uploading worker statistics summary to S3...");
        s3.putObject(new PutObjectRequest(BUCKET_NAME, WORKER_STATS_FILENAME, statsFile));
        System.out.println("Finished uploading worker statistics file to S3.");
    }

    /**
     * Initializes this instance.
     */
    private static void init() {
        workerStatistics = new HashMap<>();
        requests = new ConcurrentHashMap<>();
        shouldTerminate = new AtomicBoolean();
        shouldTerminate.set(false);
        shouldProcessRequests = new AtomicBoolean();
        shouldProcessRequests.set(true);
        numOfActiveWorkers = new AtomicInteger();
        numOfActiveWorkers.set(0);
        pendingTweets = new AtomicInteger();
        pendingTweets.set(0);
        firstRequestReceived = false;
        firstWorkerRunning = new AtomicBoolean();
        firstWorkerRunning.set(false);
        pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        // initiate connection to S3
        s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);

        // initiate connection to EC2
        ec2 = new AmazonEC2Client();
        ec2.setRegion(usEast1);

        // initiate connection to SQS
        sqs = new AmazonSQSClient();
        sqs.setRegion(usEast1);

        System.out.println("Manager running...");
    }

    /**
     *
     * @return returns true if should continue to process incoming requests in the UPSTREAM queue.
     */
    private static boolean shouldProcessRequests() {
        return shouldProcessRequests.get();
    }

    private static void compileAndSendResults(String key, String results) {
        String output = "<id>" + key + "</id>" + results;
        File file = new File(key + RESULTS_FILENAME_SUFFIX);
        FileWriter fw = null;
        try {
            fw = new FileWriter(file);
            fw.write(output);
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.print("Finished compiling results file for Local id " + key + " , uploading to S3... ");
        s3.putObject(new PutObjectRequest(BUCKET_NAME, key + RESULTS_FILENAME_SUFFIX, file));
        System.out.println("Finished uploading to S3 the results file for Local id: " + key);
        sqs.sendMessage(downstreamURL, key + "done");
    }

    /**
     * Creates a new EC2 instance for a Worker.
     */
    private static void createWorker() {
        // start a Worker instance
        try {
            RunInstancesRequest request = new RunInstancesRequest("ami-37d0c45d", 1, 1);
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
            numOfActiveWorkers.incrementAndGet();

            /*
            Wait for the first Worker instance to run. This is important - if there are pending tweets, but all Worker
            instances are dead (due to some AWS malfunction), you need to raise new Worker instances to handle all of
            the pending jobs.
             */
            if (!firstWorkerRunning.get()){
                System.out.println("Waiting for the first worker to run...");
                List<String> tagValues = new ArrayList<>();
                tagValues.add("worker");
                Filter tagFilter = new Filter("tag:kind", tagValues);
                List<String> statusValues = new ArrayList<>();
                statusValues.add("running");
                Filter statusFilter = new Filter("instance-state-name", statusValues);
                DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withFilters(tagFilter, statusFilter);
                DescribeInstancesResult filteredInstances;
                List<Reservation> reservations;
                do {
                    filteredInstances = ec2.describeInstances(describeInstancesRequest);
                    reservations = filteredInstances.getReservations();
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } while (reservations.isEmpty());
                firstWorkerRunning.set(true);
                System.out.println("First worker running.");
            }
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Response Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }

    /**
     * A generator for user data to be attached to the EC2 instance reservation.
     * @return the user data, encoded in base-64.
     */
    private static String getUserDataScript(){
        StringBuilder sb = new StringBuilder();
        sb.append("#! /bin/bash\n");
        sb.append("aws s3 cp s3://asafbendsp/jsoup-1.8.3.jar jsoup-1.8.3.jar\n");
        sb.append("aws s3 cp s3://asafbendsp/ejml-0.23.jar ejml-0.23.jar\n");
        sb.append("aws s3 cp s3://asafbendsp/jollyday-0.4.7.jar jollyday-0.4.7.jar\n");
        sb.append("aws s3 cp s3://asafbendsp/stanford-corenlp-3.3.0-models.jar stanford-corenlp-3.3.0-models.jar\n");
        sb.append("aws s3 cp s3://asafbendsp/stanford-corenlp-3.3.0.jar stanford-corenlp-3.3.0.jar\n");
        sb.append("aws s3 cp s3://asafbendsp/Worker.jar Worker.jar\n");
        sb.append("aws s3 cp s3://asafbendsp/aws-sdk-java/lib . --recursive\n");
        sb.append("aws s3 cp s3://asafbendsp/aws-sdk-java/thirdparty/lib . --recursive\n");
        sb.append("jar xf Worker.jar\n");
        sb.append("java -cp .:./* -Xms128m -Xmx768m Worker\n");
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