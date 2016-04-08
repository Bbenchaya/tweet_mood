import java.util.LinkedList;
import java.util.List;

/**
 * Created by asafchelouche on 30/3/16.
 *
 * A container for the statistics of a single request from a Local instance.
 *
 */
class RequestStatus {

    private int numOfExpectedResults;
    private List<String> results;

    /**
     * Default constructor.
     */
    RequestStatus() {
        numOfExpectedResults = -1;
        results = new LinkedList<>();
    }

    /**
     * Query whether all of the legal links in the request have been processed, i.e. a result message has been returned
     * and registered.
     * @return return true if the request has been fully handled.
     */
    boolean hasAllResults() {
        return !(numOfExpectedResults == -1 || numOfExpectedResults > results.size());
    }

    /**
     * Sets the number of expected results to be returned to the Local instance. This is necessary because this number
     * is determined only after the instance is created, and the length of time this takes depends on the number of
     * of links in the request.
     * @param numOfExpectedResults the number of results to return to the Local instance.
     */
    void setNumOfExpectedResults(int numOfExpectedResults) {
        this.numOfExpectedResults = numOfExpectedResults;
    }

    /**
     * Registers a result of processing a single tweet from the request.
     * @param result a result that was received from a Worker instance.
     */
    void addResult(String result) {
        results.add(result);
    }

    /**
     *
     * @return returns all of the results for this request.
     */
    String getResults() {
        StringBuilder sb = new StringBuilder();
        for (String result : results) {
            sb.append("<result>");
            sb.append(result);
            sb.append("</result>\n");
        }
        return sb.toString();
    }

    /**
     * If a Worker instance experienced an error while downloading a link, adjust the number of results that should be returned.
     */
    void decrementExpectedResults() {
        numOfExpectedResults--;
    }
}
