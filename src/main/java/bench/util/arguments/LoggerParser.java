package bench.util.arguments;

import bench.util.LogCompleteHistogram;
import bench.util.LogCompleteLog;
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
            return new LogSimple( true );
        case "simpletime":
            return new LogSimple( false );
        case "latex":
            return new LogLatexTable();
        case "histo":
            return new LogCompleteHistogram( true );
        case "histotime":
            return new LogCompleteHistogram( false );
        case "logtimes":
            return new LogCompleteLog();
        case "logtimes20":
            return new LogCompleteLog( 20 );
        default:
            return null;
        }
    }
}
