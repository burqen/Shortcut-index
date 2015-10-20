package bench.queries.impl.ldbc;

import bench.Measurement;
import bench.queries.QueryDescription;
import bench.queries.impl.framework.QueryShortcut;
import bench.queries.impl.description.Query1Description;
import index.SCIndex;
import index.SCIndexDescription;
import index.SCResult;
import index.btree.util.SeekerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

public class Query1Shortcut extends QueryShortcut
{
    private SCIndexDescription indexDescription = new SCIndexDescription( "Person", "Comment",
            "COMMENT_HAS_CREATOR", Direction.INCOMING, null, "creationDate" );

    @Override
    protected List<SCResult> doRunQuery( ReadOperations operations, Measurement measurement, long[] inputData )
            throws IOException
    {
        SCIndex index = indexes.get( indexDescription() );

        List<SCResult> indexSeekResult = new ArrayList<>();
        index.seek( SeekerFactory.scanner(), indexSeekResult );
        Iterator<SCResult> resultIterator = indexSeekResult.iterator();
        while ( resultIterator.hasNext() )
        {
            SCResult result = resultIterator.next();
            if ( filterResultRow( result ) )
            {
                resultIterator.remove();
            }
        }
        return indexSeekResult;
    }

    @Override
    protected boolean filterResultRow( SCResult resultRow )
    {
        return false;
    }

    @Override
    public SCIndexDescription indexDescription()
    {
        return indexDescription;
    }

    @Override
    public QueryDescription queryDescription()
    {
        return Query1Description.instance;
    }
}
