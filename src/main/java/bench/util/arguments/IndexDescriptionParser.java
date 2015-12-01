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
                        new SCIndexDescription( "Person", "Comment", "COMMENT_HAS_CREATOR", Direction.INCOMING, null,
                                "creationDate" ),
                        new SCIndexDescription( "Person", "Post", "LIKES_POST", Direction.OUTGOING, "creationDate",
                                null ),
                        new SCIndexDescription( "University", "Person", "STUDY_AT", Direction.INCOMING, "classYear",
                                null ),
                        new SCIndexDescription( "Company", "Person","WORKS_AT", Direction.INCOMING, "workFrom", null ),
                        new SCIndexDescription( "Forum", "Post", "CONTAINER_OF", Direction.OUTGOING, null,
                                "creationDate" )
                };
        switch ( s )
        {
        case "lab":
            return descriptions[0];
        case "ldbc1":
        case "ldbc2":
            return descriptions[1];
        case "ldbc3":
            return descriptions[2];
        case "ldbc4":
            return descriptions[3];
        case "ldbc5":
            return descriptions[4];
        case "ldbc6":
            return descriptions[5];
        default:
            throw new IllegalArgumentException( "Can not create workload from argument: " + s );
        }
    }
    public static StringParser INSTANCE = new IndexDescriptionParser();
}
