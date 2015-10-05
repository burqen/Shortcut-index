package bench.queries.framework;

import bench.util.LogStrategy;

import java.io.PrintStream;

public interface Measurement
{
    void queryFinished( long elapsedTime, long rowCount );
    void close();
    boolean isClosed();
    String query();
    void report( PrintStream out, LogStrategy logStrategy );
    void error( String s );
}
