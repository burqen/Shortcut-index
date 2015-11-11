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
            return new Dataset( Config.GRAPH_DB_FOLDER, Config.LDBC_SF001, Config.INPUT_DIR_LDBC1, Environment.LDBC );
        case "ldbc10":
            return new Dataset( Config.GRAPH_DB_FOLDER, Config.LDBC_SF010, Config.INPUT_DIR_LDBC10, Environment.LDBC );
        case "lab8":
            return new Dataset( Config.GRAPH_DB_FOLDER, Config.LAB_8.dbName, Config.INPUT_DIR_LAB, Environment.LAB );
        case "lab40":
            return new Dataset( Config.GRAPH_DB_FOLDER, Config.LAB_40.dbName, Config.INPUT_DIR_LAB, Environment.LAB );
        case "lab200":
            return new Dataset( Config.GRAPH_DB_FOLDER, Config.LAB_200.dbName, Config.INPUT_DIR_LAB, Environment.LAB );
        case "lab400":
            return  new Dataset( Config.GRAPH_DB_FOLDER, Config.LAB_400.dbName, Config.INPUT_DIR_LAB, Environment.LAB );
        case "lab800":
            return  new Dataset( Config.GRAPH_DB_FOLDER, Config.LAB_800.dbName, Config.INPUT_DIR_LAB, Environment.LAB );
        default:
            return null;
        }
    }

    public static final DatasetParser INSTANCE = new DatasetParser();
}
