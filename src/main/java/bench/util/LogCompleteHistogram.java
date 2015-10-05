package bench.util;

import org.HdrHistogram.Histogram;

import java.io.PrintStream;

public class LogCompleteHistogram implements LogStrategy
{
    @Override
    public void query( PrintStream out, String query )
    {
        out.print( query + "\n" );
    }

    @Override
    public void result( PrintStream out, Histogram timeHistogram, Histogram rowHistogram )
    {
        out.print( histogramString( timeHistogram, "Run Time (ms)" ) );
        out.print( "\n" );
        out.print( histogramString( rowHistogram, "Result Rows" ) );
        out.print( "\n" );
    }

    @Override
    public void error( PrintStream out, String errorMessage )
    {
        out.print( String.format( "ERROR: %s\n", errorMessage ) );
    }

    private static String histogramString( Histogram histogram, String name )
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
