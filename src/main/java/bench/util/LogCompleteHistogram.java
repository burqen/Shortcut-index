package bench.util;

import bench.BenchConfig;
import bench.LogStrategy;
import bench.Measurement;
import bench.QueryType;
import org.HdrHistogram.Histogram;

import java.io.PrintStream;

public class LogCompleteHistogram implements LogStrategy
{

    private final boolean printRowCount;

    public LogCompleteHistogram( boolean printRowCount )
    {
        this.printRowCount = printRowCount;
    }
    @Override
    public void header( PrintStream out, BenchConfig benchConfig, Dataset dataset )
    {
        // No header
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

                if ( printRowCount )
                {
                    Histogram kernelCount = measurement.rowHistogram();
                    out.print( histogramString( kernelCount, typeName + " Result Rows" ) );
                    out.print( "\n" );
                }
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

    protected String histogramString( Histogram histogram, String name )
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
