import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
//import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public class Manager {

    private static final String bucket = "asafbendsp";
    private static final String inputFileName = "/Users/bbenchaya/Documents/Programming/DSP/tweet_mood/src/short.txt";
    private static final String objectName = "tweetLinks.txt";

    FileReader fr;
    BufferedReader br;
    Stack<String> links;
    List<Worker> workers;
    Stack<String> tweets;

    public void start() {
        File inputFile = new File(inputFileName);
        FileReader fr = null;
        try {
            fr = new FileReader(inputFile);
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
        tweets = new Stack<>();
        Document doc = null;
        Elements content;
        System.out.println("Parsing tweets");
        FileWriter fr2 = null;
        try {
            fr2 = new FileWriter("tweets.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter bw2 = null;
        bw2 = new BufferedWriter(fr2);

        for (String link : links) {
            try {
                doc = Jsoup.connect(link).get();
            } catch (IOException e) {
                e.printStackTrace();
            }
            content = doc.select("title");
            //System.out.println(content.text());
            tweets.push(content.text());
            try {
                bw2.write(content.text());
                bw2.write('\n');
            } catch (IOException e) {
                e.printStackTrace();
            }
//            System.out.println(content.text());
        }

        try {
            bw2.flush();
            br.close();
            bw2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Worker worker = new Worker(tweets);
        System.out.println("Working...");
        worker.work();
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