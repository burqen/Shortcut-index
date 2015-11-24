package bench.util;

import bench.BenchConfig;
import bench.LogStrategy;
import bench.Measurement;
import org.HdrHistogram.Histogram;

import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;

import static bench.QueryType.KERNEL;
import static bench.QueryType.SHORTCUT;

public class LogLatexTable implements LogStrategy
{

    private boolean hasWrittenHeader;

    @Override
    public void header( PrintStream out, BenchConfig benchConfig, Dataset dataset )
    {
        if ( !hasWrittenHeader )
        {
            String caption = String.format( "Result table for %s", dataset.dbName );
            String label = String.format( "tbl:result" );
            String header = String.format( "\\begin{table}\n" +
                            "\\begin{center}\n" +
                            "\\caption{%s}\n" +
                            "\\label{%s}\n" +
                            "\\begin{tabular}{ | c | c | c | c | c | }\n" +
                            "\\thickhline\n" +
                            "\\multicolumn{2}{|c|}{ Query } & Kernel (µs) & Shortcut (µs)& Speedup  \\\\ \n" +
                            "\\thickhline" , latexSafe( caption ), latexSafe( label ) );
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
        long kernelFirst;
        long kernelLast;
        double kernelAvg;
        long shortcutFirst;
        long shortcutLast;
        double shortcutAvg;
        double avgSpeedup;
        double firstSpeedup;
        double lastSpeedup;

        if ( kernel != null && !kernel.error() )
        {
            Histogram kernelTime = kernel.timeHistogram();
            kernelFirst = kernel.timeForFirstQuery();
            kernelLast = kernel.timeForLastQuery();
            kernelAvg = kernelTime.getMean();
        }
        else
        {
            kernelFirst = -1;
            kernelLast = -1;
            kernelAvg = -1;
        }

        if ( shortcut != null && !shortcut.error() )
        {
            Histogram shortcutTime = shortcut.timeHistogram();
            shortcutFirst = shortcut.timeForFirstQuery();
            shortcutLast = shortcut.timeForLastQuery();
            shortcutAvg = shortcutTime.getMean();
        }
        else
        {
            shortcutFirst = -1;
            shortcutLast = -1;
            shortcutAvg = -1;
        }

        if ( kernelFirst != -1 && shortcutFirst != -1 )
        {
            firstSpeedup = (double) kernelFirst / shortcutFirst;
        }
        else
        {
            firstSpeedup = 0;
        }

        if ( kernelLast != -1 && shortcutLast != -1 )
        {
            lastSpeedup = (double) kernelLast / shortcutLast;
        }
        else
        {
            lastSpeedup = 0;
        }

        if ( kernelAvg != -1 && shortcutAvg != -1 )
        {
            avgSpeedup = kernelAvg / shortcutAvg;
        }
        else
        {
            avgSpeedup = 0;
        }

        String format = String.format( "\\multirow{2}{*}{%s}\n" +
                                       "        & first & %,d & %,d & %,.2fx \\\\ \\cline{2-5}\n" +
                                       "        & avg & %,.0f & %,.0f & %,.2fx \\\\ \\cline{2-5}\n" +
                                       "        \\thickhline\n",
                latexSafe( queryName ),
                kernelFirst,
                shortcutFirst,
                firstSpeedup,
                kernelAvg,
                shortcutAvg,
                avgSpeedup
        );
        out.print( format );
    }

    @Override
    public void footer( PrintStream out, BenchConfig benchConfig, Dataset dataset )
    {
        // get a RuntimeMXBean reference
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();

        // get the jvm's input arguments as a list of strings
        List<String> inputArguments = runtimeMxBean.getInputArguments();

        String heapParams = "";
        for ( String param : inputArguments )
        {
            if ( param.contains( "Xmx" ) || param.contains( "Xms" ) )
            {
                heapParams = heapParams + param + " ";
            }
        }

        String.format( "Dataset: %s" +
                       ", page size: %d Bytes, cache pages: %,d, total cache size: %d MB, number of warm up runs: %d, " +
                       "input data size: %d",
                dataset.dbName,
                benchConfig.pageSize(),
                benchConfig.cachePages(),
                benchConfig.pageSize() * benchConfig.cachePages() / 1000000,
                benchConfig.numberOfWarmups(),
                benchConfig.inputSize() );

        String footer = String.format(
                "\\multicolumn{5}{|c|}{ Setup } \\\\ \\thickhline\n" +
                "\\multicolumn{2}{|c|}{Dataset} & \\multicolumn{3}{c|}{%s} \\\\ \\hline\n" +
                "\\multicolumn{2}{|c|}{Page size} & \\multicolumn{3}{c|}{%dB} \\\\ \\hline\n" +
                "\\multicolumn{2}{|c|}{Cache size} & \\multicolumn{3}{c|}{%dMB} \\\\ \\hline\n" +
                "\\multicolumn{2}{|c|}{VM params} & \\multicolumn{3}{c|}{%s} \\\\\n" +
                "\\thickhline\n" +
                "\\end{tabular}\n" +
                "\\end{center}\n" +
                "\\end{table}",
                dataset.dbName,
                benchConfig.pageSize(),
                benchConfig.pageSize() * benchConfig.cachePages() / 1000000,
                heapParams.trim()
        );

        out.print( latexSafe( footer ) );
        out.print( "\n");
    }

    private String latexSafe( String s )
    {
        StringBuilder builder = new StringBuilder();
        List<Character> symbolsToEscape = new ArrayList<>();
        symbolsToEscape.add( '%' );
        symbolsToEscape.add( '_' );

        for ( int i = 0; i < s.length(); i++ )
        {
            char c = s.charAt( i );
            if ( symbolsToEscape.contains( c ) )
            {
                builder.append( "\\" );
            }
            builder.append( c );
        }

        String result = builder.toString();

        return result;
    }
}
