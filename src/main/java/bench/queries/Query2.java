package bench.queries;

import bench.queries.framework.QueryWithPropertyOnNode;
import bench.util.Config;
import index.logical.RangeSeeker;
import index.logical.ShortcutIndexDescription;
import index.logical.ShortcutIndexProvider;
import index.logical.ShortcutIndexService;
import index.logical.TResult;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class Query2 extends QueryWithPropertyOnNode
{
    public static ShortcutIndexDescription indexDescription = new ShortcutIndexDescription( "Person", "Comment",
            "COMMENT_HAS_CREATOR", Direction.INCOMING, null, "creationDate" );

    public Query2()
    {
        super();
    }

    public Query2( ShortcutIndexProvider indexes )
    {
        super( indexes );
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
    protected boolean filterOnNodeProperty( long prop )
    {
        return false;
    }

    @Override
    public String[] inputDataHeader()
    {
        return new String[]{ "Person" };
    }

    @Override
    public String query()
    {
        return "// QUERY 2 - SEEK\n" +
               "// All comments written by person\n" +
               "MATCH (p:Person {id:{1}}) <-[r:COMMENT_HAS_CREATOR]- (c:Comment)\n" +
               "RETURN id(p), id(r), id(c), c.creationDate\n";
    }

    @Override
    protected PrimitiveLongIterator startingPoints( ReadOperations operations, long[] inputData, int firstLabel )
    {
        return new PrimitiveLongIterator()
        {
            private boolean exhausted;
            @Override
            public boolean hasNext()
            {
                return !exhausted;
            }

            @Override
            public long next()
            {
                if ( exhausted )
                {
                    throw new NoSuchElementException();
                }
                exhausted = true;
                return inputData[0];
            }
        };
    }

    @Override
    protected boolean validateRow( long startPoint, long otherNode, long relationship, long prop )
    {
        return true;
    }

    @Override
    protected void doRunQueryWithIndex( ReadOperations operations, Measurement measurement, long[] inputData )
    {
        try
        {
            int firstLabel = operations.labelGetForName( firstLabel() );

            final long start = inputData[0];

            if ( !operations.nodeHasLabel( start, firstLabel ) )
            {
                throw new IllegalArgumentException(
                        "Node[" + start + "] did not have label " + firstLabel() + " as expected. " +
                        "Use correct input file." );
            }

            ShortcutIndexService index = indexes.get( indexDescription );

            List<TResult> list = new ArrayList<>();
            index.seek( new RangeSeeker( start, null, null ), list );

            for ( TResult result : list )
            {
                long otherNode = result.getValue().getNodeId();
                long rel = result.getValue().getRelId();
                long prop = result.getKey().getProp();
                if ( validateRow( start, otherNode, rel, prop ) )
                {
                    measurement.row();
                }
            }
        }
        catch ( EntityNotFoundException e )
        {
            e.printStackTrace();
        }
    }

    @Override
    public String inputFile()
    {
        return Config.QUERY2_PARAMETERS;
    }
}
