package bench;

import org.HdrHistogram.Histogram;

public interface Measurement
{
    void queryFinished( long elapsedTime, long rowCount );
    boolean error();
    String errorMessage();
    void error( String s );
    Histogram timeHistogram();
    Histogram rowHistogram();
    long timeForFirstQuery();
    long[] completeTimesLog();
}
