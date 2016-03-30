import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public class Manager {

    private static final String bucket = "asafbendsp";
    private static final String inputFileName = "/Users/bbenchaya/Documents/Programming/DSP/tweet_mood/src/short.txt";
//    private static final String inputFileName = "/Users/asafchelouche/programming/tweet_mood/src/short.txt";
    private static final String objectName = "tweetLinks.txt";
    private static double jobsPerWorker;

    FileReader fr;
    BufferedReader br;
    Stack<String> links;
    List<Worker> workers;
    SQS<String> jobs;
    SQS<Result> results;
    boolean done;

    public Manager(SQS jobs, SQS results, int n) {
        jobsPerWorker = n;
        this.jobs = jobs;
        this.results = results;
        done = false;
    }

    boolean isDone() {
        return done;
    }

    public void start() {
        //get the tweet links path, and obtain the file from S3
        String job = jobs.queue.poll();

        //download the tweets themselves and parse them
        FileReader fr = null;
        try {
            fr = new FileReader(job);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(fr);
        Stack<String> links = new Stack<>();
        String line;
        try {
            while ((line = br.readLine()) != null)
                links.push(line);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Document doc = null;
        Elements content;
        System.out.print("Parsing tweets... ");
        for (String link : links) {
            try {
                doc = Jsoup.connect(link).get();
            } catch (IOException e) {
                e.printStackTrace();
            }
            content = doc.select("title");
            jobs.queue.add(content.text());
//            System.out.println(content.text());
        }
        System.out.println("Done.");

        //determine the number of workers to initiate
        workers = new LinkedList<>();
        int numOfWorkers = (int) Math.ceil(jobs.queue.size() / jobsPerWorker);
        System.out.println("Creating workers and sending them to work...");
        for (int i = 0; i < numOfWorkers; i++) {
            Worker worker = new Worker(jobs, results);
            workers.add(worker);
            worker.work();
        }

        //gather and compile results
        while (true) {
            if (results.queue.size() < links.size())
                continue;
            break;
        }

        File file = new File("results.txt");
        FileWriter fw = null;
        try {
            fw = new FileWriter(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (Result result : results.queue) {
            try {
                fw.write(result.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        jobs.queue.add("results.txt");
        done = true;
    }


//    public static void main(String[] args) throws IOException {
//        AmazonS3 s3 = new AmazonS3Client();
//        Region usEase1 = Region.getRegion(Regions.US_EAST_1);
//        s3.setRegion(usEase1);
//        System.out.println("Manager client running...");
//        try {
//            System.out.println("Downloading tweet links file");
//            File file = new File(inputFileName);
////            s3.putObject(new PutObjectRequest(bucket, objectName, file));
//            System.out.println("Downloading an object");
//            S3Object object = s3.getObject(new GetObjectRequest(bucket, objectName));
//            System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
//            displayTextInputStream(object.getObjectContent());
//        } catch (AmazonServiceException ase) {
//            System.out.println("Caught an AmazonServiceException, which means your request made it "
//                    + "to Amazon S3, but was rejected with an error response for some reason.");
//            System.out.println("Error Message:    " + ase.getMessage());
//            System.out.println("HTTP Status Code: " + ase.getStatusCode());
//            System.out.println("AWS Error Code:   " + ase.getErrorCode());
//            System.out.println("Error Type:       " + ase.getErrorType());
//            System.out.println("Request ID:       " + ase.getRequestId());
//        } catch (AmazonClientException ace) {
//            System.out.println("Caught an AmazonClientException, which means the client encountered "
//                    + "a serious internal problem while trying to communicate with S3, "
//                    + "such as not being able to access the network.");
//            System.out.println("Error Message: " + ace.getMessage());
//        }
//    }
//
//    /**
//     * Displays the contents of the specified input stream as text.
//     *
//     * @param input
//     *            The input stream to display as text.
//     *
//     * @throws IOException
//     */
//    private static void displayTextInputStream(InputStream input) throws IOException {
//        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
//        while (true) {
//            String line = reader.readLine();
//            if (line == null) break;
//
//            System.out.println("    " + line);
//        }
//        System.out.println();
//    }

}