package bench;

import bench.queries.QueryDescription;
import bench.util.Dataset;
import bench.util.ResultRow;
import org.HdrHistogram.Histogram;

import java.io.PrintStream;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class BenchLogger implements Logger
{
    private SortedMap<String, ResultRow> resultsToReport;
    private final PrintStream out;
    private final BenchConfig benchConfig;
    private final Dataset dataset;

    public BenchLogger( PrintStream out, BenchConfig benchConfig, Dataset dataset ) {
        this.out = out;
        this.benchConfig = benchConfig;
        this.dataset = dataset;
        resultsToReport = new TreeMap<>();
    }

    public Measurement startQuery( QueryDescription queryDescription, QueryType queryType )
    {
        Measurement measurement = new Measurement()
        {
            private boolean error;
            private long firstQueryFinished;
            private long lastQueryFinished;
            private String errorMessage;
            private Histogram timeHistogram = new Histogram( TimeUnit.MICROSECONDS.convert( 1, TimeUnit.MINUTES ), 5 );
            private Histogram rowHistogram = new Histogram( 5 );

            @Override
            public void queryFinished( long elapsedTime, long rowCount )
            {
                timeHistogram.recordValue( elapsedTime );
                rowHistogram.recordValue( rowCount );
            }

            @Override
            public void error( String s )
            {
                error = true;
                errorMessage = s;
            }

            @Override
            public boolean error()
            {
                return error;
            }

            @Override
            public String errorMessage()
            {
                return errorMessage;
            }

            @Override
            public Histogram timeHistogram()
            {
                return timeHistogram;
            }

            @Override
            public Histogram rowHistogram()
            {
                return rowHistogram;
            }

            @Override
            public void firstQueryFinished( long elapsedTime )
            {
                firstQueryFinished = elapsedTime;
            }

            @Override
            public void lastQueryFinished( long elapsedTime )
            {
                lastQueryFinished = elapsedTime;
            }

            @Override
            public long timeForFirstQuery()
            {
                return firstQueryFinished;
            }

            @Override
            public long timeForLastQuery()
            {
                return lastQueryFinished;
            }
        };
        ResultRow resultRow = resultsToReport.get( queryDescription.queryName() );
        if ( resultRow == null )
        {
            resultRow = new ResultRow( queryDescription );
            resultsToReport.put( queryDescription.queryName(), resultRow );
        }
        resultRow.addMeasurement( measurement, queryType );
        return measurement;
    }

    public void report( LogStrategy logStrategy )
    {
        logStrategy.header( out, benchConfig, dataset );
        for ( ResultRow resultRow : resultsToReport.values() )
        {
            logStrategy.reportRow( out, resultRow );
        }
        logStrategy.footer( out, benchConfig, dataset );
        out.print( "\n" );
    }

    @Override
    public void close()
    {
        out.close();
    }
}
