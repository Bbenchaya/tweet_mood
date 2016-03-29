/**
 * Created by asafchelouche on 27/3/16.
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Stack;

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

public class Worker {

    private Properties sentimentProps;
    private Properties entityProps;
    private StanfordCoreNLP  sentimentPipeline;
    private StanfordCoreNLP NERPipeline;
    private SQS jobs;
    private SQS results;


    public Worker(SQS jobs, SQS results) {
        this.jobs = jobs;
        this.results = results;
        sentimentProps = new Properties();
        entityProps = new Properties();
        sentimentProps.put("annotators", "tokenize, ssplit, parse, sentiment");
        entityProps.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        sentimentPipeline =  new StanfordCoreNLP(sentimentProps);
        NERPipeline =  new StanfordCoreNLP(entityProps);
    }


    public int findSentiment(String tweet) {
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

    public void printEntities(String tweet){
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(tweet);

        // run all Annotators on this text
        NERPipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);
                System.out.println("\t-" + word + ":" + ne);
            }
        }

    }

    public String extractEntities(String tweet){
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


    public void work() {
        // work on the tweets and add the result to an SQS
        while (true) {
            String job = (String) jobs.queue.poll();
            if (job == null)
                break;
            int sentiment = findSentiment(job);
            System.out.println("Sentiment: " + sentiment);
            String entities = extractEntities(job);
            System.out.println("Entities: " + entities);
            Result result = new Result();
            result.setTweet(job);
            result.setSentiment(sentiment);
            result.setEntities(entities);
            results.queue.add(result);
        }
    }
}
