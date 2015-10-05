package bench.util;

import org.HdrHistogram.Histogram;

import java.io.PrintStream;

public interface LogStrategy
{
    void query( PrintStream out, String query );

    void result( PrintStream out, Histogram timeHistogram, Histogram rowHistogram );

    void error( PrintStream out, String errorMessage );
}
