package bench;

import bench.queries.framework.Measurement;
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

    public Measurement startQuery( String queryToMeasure )
    {
        Measurement measurement = new Measurement()
        {
            boolean closed;
            boolean error;
            String errorMessage;
            String query = queryToMeasure;
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
                return query;
            }

            @Override
            public void report( PrintStream out )
            {
                out.print( query + "\n" );
                if ( !error )
                {
                    out.print( histogramString( timeHistogram, "Run Time (ms)" ) );
                    out.print( "\n" );
                    out.print( histogramString( rowHistogram, "Result Rows" ) );
                    out.print( "\n" );
                }
                else
                {
                    out.print( String.format( "ERROR: %s\n", errorMessage ) );
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

    public boolean report()
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
            measurement.report( out );
        }
        return measurementsToReport.isEmpty();
    }

    public static String histogramString( Histogram histogram, String name )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( String.format( "\t\t%15s\n", name ) )
                .append( String.format( "\t\t%15s\t: %10d\n", "COUNT", histogram.getTotalCount() ) )
                .append( String.format( "\t\t%15s\t: %10.0f\n", "MEAN", histogram.getMean() ) )
                .append( String.format( "\t\t%15s\t: %10d\n", "MIN", histogram.getMinValue() ) )
                .append( String.format( "\t\t%15s\t: %10d\n", "MAX", histogram.getMaxValue() ) )
                .append( String.format( "\t\t%15s\t: %10d\n", "50th PERCENTILE", histogram.getValueAtPercentile( 50 ) ) )
                .append( String.format( "\t\t%15s\t: %10d\n", "90th PERCENTILE", histogram.getValueAtPercentile( 90 ) ) )
                .append( String.format( "\t\t%15s\t: %10d\n", "95th PERCENTILE", histogram.getValueAtPercentile( 95 )
                ) )
                .append(
                        String.format( "\t\t%15s\t: %10d\n", "99th PERCENTILE", histogram.getValueAtPercentile( 99 ) ) )
                ;
        return sb.toString();
    }
}
