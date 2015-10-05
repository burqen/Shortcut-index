package bench.util;

import org.HdrHistogram.Histogram;

import java.io.PrintStream;

public class LogLatexTable implements LogStrategy
{
    @Override
    public void query( PrintStream out, String query )
    {

    }

    @Override
    public void result( PrintStream out, Histogram timeHistogram, Histogram rowHistogram )
    {

    }

    @Override
    public void error( PrintStream out, String errorMessage )
    {

    }

    \begin{table}
    \caption{Some Result}
    \label{someResult}
    \begin{tabular}{ | c | c | c | c | c | }
    \hline
    \multicolumn{2}{|c|}{ Query } & Neo4j & Shortcut & Speedup  \\
        \hline
    \multirow{2}{*}{Query1} & avg & [NeoAvg] & [SCAvg] & 1x \\ \cline{2-5}
    & 95th & [Neo95] & [SC95] & 1x \\
        \hline
    \end{tabular}
    \end{table}
}
