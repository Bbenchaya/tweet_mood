/**
 * Created by asafchelouche on 6/4/16.
 */
public class WorkerStatistics {

    private int dropped;
    private int successful;

    WorkerStatistics() {
        dropped = 0;
        successful = 0;
    }

    void addDropped() {
        dropped++;
    }

    void addSuccessful() {
        successful++;
    }

    int getDropped() {
        return dropped;
    }

    int getSuccessful() {
        return successful;
    }
}
