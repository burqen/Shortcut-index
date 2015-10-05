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
    void firstQueryFinished( long elapsedTime );
    void lastQueryFinished( long elapsedTime );
    long timeForFirstQuery();
    long timeForLastQuery();
}
