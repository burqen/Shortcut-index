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
            private String errorMessage;
            private Histogram timeHistogram = new Histogram( TimeUnit.MICROSECONDS.convert( 1, TimeUnit.MINUTES ), 5 );
            private Histogram rowHistogram = new Histogram( 5 );
            public boolean firstQuery = true;

            @Override
            public void queryFinished( long elapsedTime, long rowCount )
            {
                if ( firstQuery )
                {
                    firstQueryFinished = elapsedTime;
                    firstQuery = false;
                }
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
            public long timeForFirstQuery()
            {
                return firstQueryFinished;
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
