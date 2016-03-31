import java.util.LinkedList;
import java.util.List;

/**
 * Created by asafchelouche on 30/3/16.
 */
public class RequestStatus {

    private int numOfExpectedResults;
    private List<String> results;

    public RequestStatus() {
        numOfExpectedResults = -1;
        results = new LinkedList<>();
    }

    public int numOfResults() {
        return results.size();
    }

    public boolean hasAllResults() {
        return numOfExpectedResults == -1 || numOfExpectedResults > numOfResults();
    }

    public void setNumOfExpectedResults(int numOfExpectedResults) {
        this.numOfExpectedResults = numOfExpectedResults;
    }

    public void addResult(String result) {
        results.add(result);
    }

    public String getResults() {
        StringBuilder sb = new StringBuilder();
        for (String result : results) {
            sb.append("<result>");
            sb.append(result);
            sb.append("</result>\n");
        }
        return sb.toString();
    }
}
