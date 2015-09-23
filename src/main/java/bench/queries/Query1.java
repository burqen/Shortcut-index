package bench.queries;

import bench.queries.framework.QueryWithPropertyOnNode;
import bench.util.Config;
import index.logical.BTScanner;
import index.logical.ShortcutIndexDescription;
import index.logical.ShortcutIndexProvider;
import index.logical.ShortcutIndexService;
import index.logical.TResult;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

public class Query1 extends QueryWithPropertyOnNode
{
    public static ShortcutIndexDescription indexDescription = new ShortcutIndexDescription( "Person", "Comment",
            "COMMENT_HAS_CREATOR", Direction.INCOMING, null, "creationDate" );

    public Query1()
    {
        super();
    }

    public Query1( ShortcutIndexProvider indexes )
    {
        super( indexes );
    }

    @Override
    protected boolean filterOnNodeProperty( long prop )
    {
        return false;
    }

    @Override
    public String query()
    {
        return "// QUERY 1 - SCAN\n" +
               "// All comments written by all persons\n" +
               "MATCH (p:Person) <-[r:COMMENT_HAS_CREATOR]- (c:Comment)\n" +
               "RETURN id(p), id(r), id(c), c.creationDate;";
    }

    @Override
    protected PrimitiveLongIterator startingPoints( ReadOperations operations, long[] inputData, int firstLabel )
    {
        return operations.nodesGetForLabel( firstLabel );
    }

    @Override
    protected boolean validateRow( long startPoint, long otherNode, long relationship, long prop )
    {
        return true;
    }

    @Override
    protected String firstLabel()
    {
        return "Person";
    }

    @Override
    protected String secondLabel()
    {
        return "Comment";
    }

    @Override
    protected String relType()
    {
        return "COMMENT_HAS_CREATOR";
    }

    @Override
    protected Direction direction()
    {
        return Direction.INCOMING;
    }

    @Override
    protected String propKey()
    {
        return "creationDate";
    }

    @Override
    public String[] inputDataHeader()
    {
        return Config.NO_HEADER;
    }

    protected void doRunQueryWithIndex( ReadOperations operations, Measurement measurement, long[] inputData )
    {
        ShortcutIndexService index = indexes.get( indexDescription );

        List<TResult> results = new ArrayList<>();
        index.seek( new BTScanner(), results );
        for ( TResult result : results )
        {
            long start = result.getKey().getId();
            long otherNode = result.getValue().getNodeId();
            long rel = result.getValue().getRelId();
            long prop = result.getKey().getProp();
            if ( validateRow( start, otherNode, rel, prop ) )
            {
                measurement.row();
            }
        }
    }

    @Override
    public String inputFile()
    {
        return Config.NO_INPUT;
    }
}
