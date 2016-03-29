/**
 * Created by asafchelouche on 25/3/16.
 */

import java.io.*;

import java.io.File;
import java.util.Scanner;

public class Local {

    private static final String bucket = "asafbendsp";
    private static final String inputFileName =  "/Users/bbenchaya/Documents/Programming/DSP/tweet_mood/src/tweetLinks.txt";
//    private static final String inputFileName = "/Users/asafchelouche/programming/tweet_mood/src/tweetLinks.txt";
    private static final String objectName = "tweetLinks.txt";

    private static final String HEADER = "<html>\n\t<head>\n\t\t<title>DSP 162, assignment 1</title>\n\t</head>\n\t<body>";


    private static final String FOOTER = "\n\t</body>\n</html>";

    public static void main(String[] args) throws IOException {
        int n;
//        if (args.length < 6 || args.length > 7) {
//            System.out.println("Usage: java -jar <yourjar.jar> <inputFileName.txt> <outputFileName.html> <n> <terminate>");
//            exit();
//        }
        n = 4;
        Manager manager = null;

        //upload the tweet links list to S3
        File file = new File(inputFileName);

        //add file link to jobs
        SQS jobs = new SQS();
        jobs.queue.add(inputFileName);

        SQS results = new SQS();

        //check if a Manager exists on EC2, if not initialize it
        if (manager == null) {
            manager = new Manager(jobs, results, n);
        }

        //download the list of tweets
        manager.start();


        //check that the manager is done

        String resultPath = (String) jobs.queue.poll();

        File output = new File("output.html");
        FileWriter fw = new FileWriter(output);
        fw.write(HEADER);


        Scanner scanner = new Scanner(new File(resultPath));
        scanner.useDelimiter("<delimiter>");
        //tokenize the results file and process each token one at a time
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