package bench.queries;

import java.io.PrintStream;

public interface Measurement
{
    void countSuccess( long elapsedTime );
    void row();
    void close();
    boolean isClosed();
    String query();
    void report( PrintStream out );
}
