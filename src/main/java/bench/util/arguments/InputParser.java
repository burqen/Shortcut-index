package bench.util.arguments;

import bench.util.Config;
import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.StringParser;

public class InputParser extends StringParser
{
    @Override
    public Object parse( String s ) throws ParseException
    {
        switch ( s )
        {
        case "ldbc1":
            return Config.INPUT_DIR_LDBC1;
        case "ldbc10":
            return Config.INPUT_DIR_LDBC10;
        case "lab":
            return Config.INPUT_DIR_LAB;
        default:
            return null;
        }
    }

    public static final InputParser INSTANCE = new InputParser();
}
