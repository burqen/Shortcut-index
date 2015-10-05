package bench.util;

import bench.LogStrategy;
import bench.QueryType;
import bench.Measurement;
import org.HdrHistogram.Histogram;

import java.io.PrintStream;

public class LogCompleteHistogram implements LogStrategy
{
    @Override
    public void header( PrintStream out )
    {
        // No header
    }

    @Override
    public void reportRow( PrintStream out, ResultRow resultRow )
    {
        out.print( resultRow.query().cypher() );
        out.print( "\n" );

        reportType( out, resultRow.measurement( QueryType.KERNEL ), "Kernel" );
        reportType( out, resultRow.measurement( QueryType.SHORTCUT ), "Shortcut" );
    }

    private void reportType( PrintStream out, Measurement measurement, String typeName )
    {
        if ( measurement != null )
        {
            if ( !measurement.error() )
            {
                Histogram kernelTime = measurement.timeHistogram();
                out.print( histogramString( kernelTime, typeName + " Run Time (µs)" ) );
                out.print( "\n" );

                Histogram kernelCount = measurement.rowHistogram();
                out.print( histogramString( kernelCount, typeName + " Result Rows" ) );
                out.print( "\n" );
            }
            else
            {
                out.print( typeName + " ERROR: " + measurement.errorMessage() );
            }
        }
        else
        {
            out.print( "Measurement is missing for " + typeName );
        }
    }

    @Override
    public void footer( PrintStream out )
    {
        // No footer
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
