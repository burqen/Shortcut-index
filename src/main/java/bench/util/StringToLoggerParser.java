package bench.util;

import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.StringParser;

public class StringToLoggerParser extends StringParser
{
    public static StringParser INSTANCE = new StringToLoggerParser();

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
