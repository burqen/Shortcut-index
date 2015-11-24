package bench.util.arguments;

import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.StringParser;
import index.SCIndexDescription;

import org.neo4j.graphdb.Direction;

public class IndexDescriptionParser extends StringParser
{
    @Override
    public Object parse( String s ) throws ParseException
    {
        SCIndexDescription[] descriptions = new SCIndexDescription[]
                {
                        new SCIndexDescription( "Person", "Comment", "CREATED", Direction.OUTGOING, null, "date" ),
                };
        switch ( s )
        {
        case "lab":
            return descriptions[0];
        default:
            throw new IllegalArgumentException( "Can not create workload from argument: " + s );
        }
    }

    public static StringParser INSTANCE = new IndexDescriptionParser();
}
