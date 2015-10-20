package bench;

import bench.util.Dataset;
import bench.util.ResultRow;

import java.io.PrintStream;

public interface LogStrategy
{
    void header( PrintStream out, BenchConfig benchConfig, Dataset dataset );

    void reportRow( PrintStream out, ResultRow resultRow );

    void footer( PrintStream out );
}
