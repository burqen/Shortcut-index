package bench.queries.impl;

import bench.Measurement;
import bench.queries.QueryDescription;
import bench.queries.framework.QueryShortcut;
import bench.queries.impl.description.Query1Description;
import index.SCIndexDescription;
import index.legacy.BTScanner;
import index.ShortcutIndexProvider;
import index.legacy.LegacySCIndex;
import index.legacy.TResult;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

public class Query1Shortcut extends QueryShortcut
{
    public static SCIndexDescription indexDescription = new SCIndexDescription( "Person", "Comment",
            "COMMENT_HAS_CREATOR", Direction.INCOMING, null, "creationDate" );

    public Query1Shortcut( ShortcutIndexProvider indexes )
    {
        super( indexes );
    }

    @Override
    protected List<TResult> doRunQuery( ReadOperations operations, Measurement measurement, long[] inputData )
    {
        LegacySCIndex index = indexes.get( indexDescription );

        List<TResult> indexSeekResult = new ArrayList<>();
        index.seek( new BTScanner(), indexSeekResult );
        Iterator<TResult> resultIterator = indexSeekResult.iterator();
        while ( resultIterator.hasNext() )
        {
            TResult result = resultIterator.next();
            if ( filterResultRow( result ) )
            {
                resultIterator.remove();
            }
        }
        return indexSeekResult;
    }

    @Override
    protected boolean filterResultRow( TResult resultRow )
    {
        return false;
    }

    @Override
    public QueryDescription queryDescription()
    {
        return Query1Description.instance;
    }
}
