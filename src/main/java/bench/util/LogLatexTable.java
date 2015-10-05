package bench.util;

import bench.LogStrategy;
import bench.Measurement;
import org.HdrHistogram.Histogram;

import java.io.PrintStream;

import static bench.QueryType.KERNEL;
import static bench.QueryType.SHORTCUT;

public class LogLatexTable implements LogStrategy
{

    private boolean hasWrittenHeader;

    @Override
    public void header( PrintStream out )
    {
        if ( !hasWrittenHeader )
        {
            String header = String.format( "\\begin{table}\n" +
                            "\\begin{center}\n" +
                            "\\caption{%s}\n" +
                            "\\label{%s}\n" +
                            "\\begin{tabular}{ | c | c | c | c | c | }\n" +
                            "\\hline\n" +
                            "\\multicolumn{2}{|c|}{ Query } & Neo4j (µs) & Shortcut (µs)& Speedup  \\\\ \n" +
                            "\\hline" , "Result table", "tbl:result" );
            out.print( header );
            out.print( "\n" );
            hasWrittenHeader = true;
        }
    }

    @Override
    public void reportRow( PrintStream out, ResultRow resultRow )
    {
        Measurement kernel = resultRow.measurement( KERNEL );
        Measurement shortcut = resultRow.measurement( SHORTCUT );

        String queryName = resultRow.query().queryName();
        double kernelAvg;
        double shortcutAvg;
        long kernelPercentile;
        long shortcutPercentile;
        int speedup;

        if ( kernel != null && !kernel.error() )
        {
            Histogram kernelTime = kernel.timeHistogram();
            kernelAvg = kernelTime.getMean();
            kernelPercentile = kernelTime.getValueAtPercentile( 95 );
        }
        else
        {
            kernelAvg = -1;
            kernelPercentile = -1;
        }

        if ( shortcut != null && !shortcut.error() )
        {
            Histogram shortcutTime = shortcut.timeHistogram();
            shortcutAvg = shortcutTime.getMean();
            shortcutPercentile = shortcutTime.getValueAtPercentile( 95 );
        }
        else
        {
            shortcutAvg = -1;
            shortcutPercentile = -1;
        }

        if ( kernelAvg != -1 && shortcutAvg != -1 )
        {
            speedup = (int) (kernelAvg / shortcutAvg);
        }
        else
        {
            speedup = 0;
        }

        String format = String.format( "\\multirow{2}{*}{%s} & avg & %f & %f & \\multirow{2}{*}{%dx} \\\\ \\cline{2-4}\n" +
                                       "        & 95th & %d & %d & \\\\ \n" +
                                       "        \\hline\n",
                queryName,
                kernelAvg,
                shortcutAvg,
                speedup,
                kernelPercentile,
                shortcutPercentile
        );
        out.print( format );
    }

    @Override
    public void footer( PrintStream out )
    {
        String footer = "\\end{tabular}\n" +
                        "\\end{center}\n" +
                        "\\end{table}";
        out.print( footer );
        out.print( "\n");
    }
}
