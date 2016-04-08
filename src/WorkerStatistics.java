/**
 * Created by asafchelouche on 6/4/16.
 *
 * A container for the statistics of a single Worker instance.
 *
 */
public class WorkerStatistics {

    private int dropped;
    private int successful;

    /**
     * Default constructor.
     */
    WorkerStatistics() {
        dropped = 0;
        successful = 0;
    }

    /**
     * The Worker experienced an error while trying to download a link
     */
    void addDropped() {
        dropped++;
    }

    /**
     * The Worker successfully downloaded a link.
     */
    void addSuccessful() {
        successful++;
    }

    /**
     *
     * @return returns the number of links that caused errors while attempting to download them.
     */
    int getDropped() {
        return dropped;
    }

    /**
     *
     * @return returns the number of links successfully downloaded.
     */
    int getSuccessful() {
        return successful;
    }
}
