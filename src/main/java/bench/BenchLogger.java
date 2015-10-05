package bench;

import bench.queries.framework.Measurement;
import bench.queries.framework.QueryDescription;
import bench.util.LogStrategy;
import org.HdrHistogram.Histogram;

import java.io.PrintStream;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class BenchLogger
{
    private Queue<Measurement> measurementsToReport;
    private final PrintStream out;
    private String logHeader;
    private boolean hasWrittenHeader;

    public BenchLogger( PrintStream out, String logHeader ) {
        this.out = out;
        this.logHeader = logHeader;
        measurementsToReport = new LinkedList<>();
    }

    public Measurement startQuery( QueryDescription queryDescription, QueryType queryType )
    {
        Measurement measurement = new Measurement()
        {
            boolean closed;
            boolean error;
            String errorMessage;
            QueryType type = queryType;
            QueryDescription query = queryDescription;
            Histogram timeHistogram = new Histogram( TimeUnit.MILLISECONDS.convert( 10, TimeUnit.MINUTES ), 5 );
            Histogram rowHistogram = new Histogram( 5 );

            @Override
            public void queryFinished( long elapsedTime, long rowCount )
            {
                timeHistogram.recordValue( elapsedTime );
                rowHistogram.recordValue( rowCount );
            }

            @Override
            public void close()
            {
                closed = true;
            }

            @Override
            public boolean isClosed()
            {
                return closed;
            }

            @Override
            public String query()
            {
                return query.cypher();
            }

            @Override
            public void report( PrintStream out, LogStrategy logStrategy )
            {
                logStrategy.query( out, query );
                if ( !error )
                {
                    logStrategy.result( out, timeHistogram, rowHistogram );
                }
                else
                {
                    logStrategy.error( out, errorMessage );
                }
            }

            @Override
            public void error( String s )
            {
                close();
                error = true;
                errorMessage = s;
            }
        };
        measurementsToReport.add( measurement );
        return measurement;
    }

    public boolean report( LogStrategy logStrategy )
    {
        while ( !measurementsToReport.isEmpty() && measurementsToReport.peek().isClosed() )
        {
            if ( !hasWrittenHeader )
            {
                out.print( logHeader );
                out.print( "\n" );
                hasWrittenHeader = true;
            }
            Measurement measurement = measurementsToReport.poll();
            measurement.report( out, logStrategy );
        }
        return measurementsToReport.isEmpty();
    }

    public enum QueryType
    {
        KERNEL, SHORTCUT
    }
}
