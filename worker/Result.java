/**
 * Created by asafchelouche on 29/3/16.
 */
public class Result {

    private String tweet;
    private int sentiment;
    private String entities;

    public void setTweet(String tweet) {
        this.tweet = tweet;
    }

    public void setSentiment(int sentiment) {
        this.sentiment = sentiment;
    }

    public void setEntities(String entities) {
        this.entities = entities;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("<result>");
        sb.append("<tweet>");
        sb.append(tweet);
        sb.append("</tweet>");
        sb.append("<sentiment>");
        sb.append(sentiment);
        sb.append("</sentiment>");
        sb.append("<entities>");
        sb.append(entities);
        sb.append("</entities>");
        sb.append("</result><delimiter>");
        return sb.toString();
    }
}
