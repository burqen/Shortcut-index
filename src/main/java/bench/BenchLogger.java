package bench;

import bench.queries.QueryDescription;
import bench.util.ResultRow;
import org.HdrHistogram.Histogram;

import java.io.PrintStream;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class BenchLogger
{
    private SortedMap<String, ResultRow> resultsToReport;
    private final PrintStream out;

    public BenchLogger( PrintStream out ) {
        this.out = out;
        resultsToReport = new TreeMap<>();
    }

    public Measurement startQuery( QueryDescription queryDescription, QueryType queryType )
    {
        Measurement measurement = new Measurement()
        {
            boolean error;
            String errorMessage;
            Histogram timeHistogram = new Histogram( TimeUnit.MICROSECONDS.convert( 1, TimeUnit.MINUTES ), 5 );
            Histogram rowHistogram = new Histogram( 5 );

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
        logStrategy.header( out );
        for ( ResultRow resultRow : resultsToReport.values() )
        {
            logStrategy.reportRow( out, resultRow );
        }
        logStrategy.footer( out );
    }

}
