/**
 * Created by asafchelouche on 27/3/16.
 */

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;

import static java.lang.Thread.sleep;

// TODO extract the different code sections to methods

public class Worker {

    private static final String BUCKET_NAME = "asafbendsp";
    private static final String WORKER_QUEUES_FILENAME = "jobs-results.txt";
    private static Properties sentimentProps;
    private static Properties entityProps;
    private static StanfordCoreNLP  sentimentPipeline;
    private static StanfordCoreNLP NERPipeline;
    private static String jobsURL;
    private static String resultsURL;
    private static AmazonS3 s3;
    private static AmazonSQS sqs;

    public static void main(String[] args) throws IOException {
        // initialize local fields
        sentimentProps = new Properties();
        entityProps = new Properties();
        sentimentProps.put("annotators", "tokenize, ssplit, parse, sentiment");
        entityProps.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        sentimentPipeline = new StanfordCoreNLP(sentimentProps);
        NERPipeline = new StanfordCoreNLP(entityProps);

        // initiate connection to S3
        s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);

        // initiate connection to SQS
        sqs = new AmazonSQSClient();
        sqs.setRegion(usEast1);

        System.out.println("Worker running...");
//        AWSCredentials credentials = null;
//        try {
//            credentials = new ProfileCredentialsProvider().getCredentials();
//        } catch (Exception e) {
//            throw new AmazonClientException(
//                    "Cannot load the credentials from the credential profiles file. " +
//                            "Please make sure that your credentials file is at the correct " +
//                            "location (~/.aws/credentials), and is in valid format.",
//                    e);
//        }

        // get the  SQS URLs file from S3
        System.out.print("Downloading jobs/results queues' URLs file from S3... ");
        S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, WORKER_QUEUES_FILENAME));
        System.out.println("Done.");
        BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
        jobsURL = br.readLine();
        resultsURL = br.readLine();
        br.close();

        // work on the tweets and add the result to an SQS
        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(jobsURL);
        ReceiveMessageResult receiveMessageResult;
        while (true) {
//            while (sqs.getQueueAttributes(getQueueAttributesRequest).getAttributes().get("ApproximateNumberOfMessages").equals("0")) {
            while ((receiveMessageResult = sqs.receiveMessage(jobsURL)).getMessages().isEmpty()) {
                    try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            List<Message> messages = receiveMessageResult.getMessages();
            // all incoming jobs have been processed, and the last 'job' is a termination message
            if (messages.size() == 1 && messages.get(0).getBody().contains("terminate")) {
                System.out.print("Termination message received, exiting... ");
                // TODO send statistics in the results queue
                Runtime rt = Runtime.getRuntime();
                Process pr = rt.exec("shutdown -h now"); // sends a kill message to the EC2 instance
                break; // if the EC2 instance is still running, this would cause the Worker to end execution
            }
            for (Message message : messages) {
                    // analyze the tweet, create the result message and place it in 'results' SQS
                    String job = message.getBody();
                    String id = job.substring(job.indexOf("<id>") + 4, job.indexOf("</id>"));
                    String tweet = job.substring(job.indexOf("<content>") + 9, job.indexOf("</content>"));
                    int sentiment = findSentiment(tweet);
                System.out.println("Sentiment: " + sentiment);
                    String entities = extractEntities(tweet);
                System.out.println("Entities: " + entities);
                    String result = resultAsString(id, tweet, sentiment, entities);
                    sqs.sendMessage(resultsURL, result);
                    String messageReceiptHandle = message.getReceiptHandle();
//                    sqs.changeMessageVisibility(jobsURL, messageReceiptHandle, 0);
                    sqs.deleteMessage(new DeleteMessageRequest(jobsURL, messageReceiptHandle));
                }
            }
        System.out.println("Done.");
    }

    private static int findSentiment(String tweet) {
        int mainSentiment = 0;
        if (tweet != null && tweet.length() > 0) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(tweet);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
        return mainSentiment;
    }

    private static String extractEntities(String tweet){
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(tweet);

        // run all Annotators on this text
        NERPipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        StringBuilder sb = new StringBuilder();
        sb.append("[");

        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);
                if (!ne.equals("O"))
                    if(ne.equals("PERSON") || ne.equals("LOCATION") || ne.equals("ORGANIZATION"))
                        sb.append(word + ":" + ne + ",");
            }
        }
        if (sb.length() > 1)
            sb.setCharAt(sb.lastIndexOf(","), ']');
        else
            sb.append("]");
        return sb.toString();
    }

    private static String resultAsString(String id, String tweet, int sentiment, String entities) {
        StringBuilder sb = new StringBuilder();
        sb.append("<id>");
        sb.append(id);
        sb.append("</id>");
        sb.append("<tweet>");
        sb.append(tweet);
        sb.append("</tweet>");
        sb.append("<sentiment>");
        sb.append(sentiment);
        sb.append("</sentiment>");
        sb.append("<entities>");
        sb.append(entities);
        sb.append("</entities>");
        sb.append("<delimiter>");
        return sb.toString();
    }

}
