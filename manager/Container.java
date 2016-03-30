import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by asafchelouche on 30/3/16.
 */
public class Container {

    private int numOfExpectedResults;
    private List<String> results;

    public Container() {
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

}
