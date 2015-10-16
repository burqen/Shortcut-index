package bench.util.arguments;

import bench.util.LogCompleteHistogram;
import bench.util.LogLatexTable;
import bench.util.LogSimple;
import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.StringParser;

public class LoggerParser extends StringParser
{
    public static StringParser INSTANCE = new LoggerParser();

    @Override
    public Object parse( String s ) throws ParseException
    {
        switch ( s )
        {
        case "simple":
            return new LogSimple();
        case "latex":
            return new LogLatexTable();
        case "histo":
            return new LogCompleteHistogram();
        default:
            return null;
        }
    }
}
