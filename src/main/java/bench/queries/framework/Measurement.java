package bench.queries.framework;

import java.io.PrintStream;

public interface Measurement
{
    void queryFinished( long elapsedTime, long rowCount );
    void close();
    boolean isClosed();
    String query();
    void report( PrintStream out );
    void error( String s );
}
