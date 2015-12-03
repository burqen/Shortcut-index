package bench.util;

import bench.BenchConfig;
import bench.LogStrategy;
import bench.Measurement;
import bench.queries.QueryDescription;

import java.io.PrintStream;

import static bench.QueryType.KERNEL;
import static bench.QueryType.SHORTCUT;

public class LogCompleteLog implements LogStrategy
{
    private final int logFirst;

    public LogCompleteLog()
    {
        logFirst = Integer.MAX_VALUE;
    }

    public LogCompleteLog( int logFirst )
    {
        this.logFirst = logFirst;
    }

    @Override
    public void header( PrintStream out, BenchConfig benchConfig, Dataset dataset )
    {
        String runConfigurations = String.format( "Run configurations:\n" +
                                                  "Dataset: %s\n" +
                                                  "Page size: %d Bytes, Cache pages: %,d, Cache size: %d MB," +
                                                  " Warm ups: %d, Input data size: %d\n",
                dataset.dbName,
                benchConfig.pageSize(),
                benchConfig.cachePages(),
                benchConfig.pageSize() * benchConfig.cachePages() / 1000000,
                benchConfig.numberOfWarmups(),
                benchConfig.inputSize() );
        out.print( runConfigurations );
    }

    @Override
    public void reportRow( PrintStream out, ResultRow resultRow )
    {
        QueryDescription query = resultRow.query();
        String queryName = query.queryName();
        if ( resultRow.hasMeasurement( KERNEL ) )
        {
            String headline = "\n" + queryName + "\nKERNEL\n";
            Measurement measurement = resultRow.measurement( KERNEL );
            logTimes( out, measurement, headline );
        }

        if ( resultRow.hasMeasurement( SHORTCUT ) )
        {
            String headline = "\n" + queryName + "\nSHORTCUT\n";
            Measurement measurement = resultRow.measurement( SHORTCUT );
            logTimes( out, measurement, headline );
        }
    }

    private void logTimes( PrintStream out, Measurement measurement, String headline )
    {
        StringBuilder toPrint = new StringBuilder( "" );
        toPrint.append( headline );
        long[] times = measurement.completeTimesLog();
        int numberOfRowsToLog = times.length < logFirst ? times.length : logFirst;
        for ( int i = 0; i < numberOfRowsToLog; i++ )
        {
            toPrint.append( times[i] );
            toPrint.append( "\n" );
        }
        out.print( toPrint );
    }

    @Override
    public void footer( PrintStream out, BenchConfig benchConfig, Dataset dataset )
    {
        // No footer
    }
}
