import java.util.LinkedList;
import java.util.List;

/**
 * Created by asafchelouche on 30/3/16.
 */
class RequestStatus {

    private int numOfExpectedResults;
    private List<String> results;

    RequestStatus() {
        numOfExpectedResults = -1;
        results = new LinkedList<>();
    }

    private int numOfResults() {
        return results.size();
    }

    boolean hasAllResults() {
        return !(numOfExpectedResults == -1 || numOfExpectedResults > numOfResults());
    }

    void setNumOfExpectedResults(int numOfExpectedResults) {
        this.numOfExpectedResults = numOfExpectedResults;
    }

    void addResult(String result) {
        results.add(result);
    }

    String getResults() {
        StringBuilder sb = new StringBuilder();
        for (String result : results) {
            sb.append("<result>");
            sb.append(result);
            sb.append("</result>\n");
        }
        return sb.toString();
    }

    void decrementExpectedResults() {
        numOfExpectedResults--;
    }
}
