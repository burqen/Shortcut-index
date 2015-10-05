package bench;

import bench.util.ResultRow;

import java.io.PrintStream;

public interface LogStrategy
{
    void header( PrintStream out );

    void reportRow( PrintStream out, ResultRow resultRow );

    void footer( PrintStream out );
}
