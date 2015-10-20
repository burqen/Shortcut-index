package bench.util;

import org.HdrHistogram.Histogram;

public class LogSimple extends LogCompleteHistogram
{
    public LogSimple( boolean printRowCount )
    {
        super( printRowCount );
    }

    @Override
    protected String histogramString( Histogram histogram, String name )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( String.format( "\t\t%15s\n", name ) )
                .append( String.format( "\t\t%15s\t: %10d\n", "COUNT", histogram.getTotalCount() ) )
                .append( String.format( "\t\t%15s\t: %10.0f\n", "MEAN", histogram.getMean() ) );
        return sb.toString();
    }
}
