package bench.util.arguments;

import bench.Environment;
import bench.util.Config;
import bench.util.Dataset;
import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.StringParser;

public class DatasetParser extends StringParser
{
    @Override
    public Object parse( String s ) throws ParseException
    {
        switch ( s )
        {
        case "ldbc1":
            return new Dataset( Config.GRAPH_DB_FOLDER, Config.LDBC_SF001, Environment.LDBC );
        case "lab10":
            return new Dataset( Config.GRAPH_DB_FOLDER, Config.LAB_10.dbName, Environment.LAB );
        case "lab50":
            return new Dataset( Config.GRAPH_DB_FOLDER, Config.LAB_50.dbName, Environment.LAB );
        case "lab200":
            return new Dataset( Config.GRAPH_DB_FOLDER, Config.LAB_200.dbName, Environment.LAB );
        case "lab500":
            return  new Dataset( Config.GRAPH_DB_FOLDER, Config.LAB_500.dbName, Environment.LAB );
        case "lab1000":
            return  new Dataset( Config.GRAPH_DB_FOLDER, Config.LAB_1000.dbName, Environment.LAB );
        default:
            return null;
        }
    }

    public static final DatasetParser INSTANCE = new DatasetParser();
}
