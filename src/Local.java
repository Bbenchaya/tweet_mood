/**
 * Created by asafchelouche on 25/3/16.
 */

import java.io.*;
import java.util.LinkedList;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.File;
import java.util.List;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;

public class Local {

    private static final String bucket = "asafbendsp";
    private static final String inputFileName = "/Users/asafchelouche/programming/tweet_mood/src/tweetLinks.txt";
    private static final String objectName = "tweetLinks.txt";

    public static void main(String[] args) throws IOException {

        Manager manager = new Manager();
        manager.start();



//        AmazonS3 s3 = new AmazonS3Client();
//        Region usEase1 = Region.getRegion(Regions.US_EAST_1);
//        s3.setRegion(usEase1);
//        System.out.println("Local client running...");
//        try {
//            System.out.println("Uploading a new object to S3 from a file\n");
//            File file = new File(inputFileName);
//            s3.putObject(new PutObjectRequest(bucket, objectName, file));
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

//        deployManager();
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