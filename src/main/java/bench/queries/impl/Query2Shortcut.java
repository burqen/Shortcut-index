package bench.queries.impl;

import bench.Measurement;
import bench.queries.QueryDescription;
import bench.queries.framework.QueryShortcut;
import bench.queries.impl.description.Query2Description;
import index.logical.RangeSeeker;
import index.logical.ShortcutIndexDescription;
import index.logical.ShortcutIndexProvider;
import index.logical.ShortcutIndexService;
import index.logical.TResult;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class Query2Shortcut extends QueryShortcut
{
    public static ShortcutIndexDescription indexDescription = new ShortcutIndexDescription( "Person", "Comment",
            "COMMENT_HAS_CREATOR", Direction.INCOMING, null, "creationDate" );

    public Query2Shortcut( ShortcutIndexProvider indexes )
    {
        super( indexes );
    }

    @Override
    protected List<TResult> doRunQuery( ReadOperations operations, Measurement measurement, long[] inputData )
    {
        List<TResult> indexSeekResult = new ArrayList<>();
        try
        {
            int firstLabel = operations.labelGetForName( indexDescription.firstLabel );

            final long start = inputData[0];

            if ( !operations.nodeHasLabel( start, firstLabel ) )
            {
                throw new IllegalArgumentException(
                        "Node[" + start + "] did not have label " + indexDescription.firstLabel + " as expected. " +
                        "Use correct input file." );
            }

            ShortcutIndexService index = indexes.get( indexDescription );

            index.seek( new RangeSeeker( start, null, null ), indexSeekResult );

            Iterator<TResult> resultIterator = indexSeekResult.iterator();
            while ( resultIterator.hasNext() )
            {
                TResult result = resultIterator.next();
                if ( filterResultRow( result ) )
                {
                    resultIterator.remove();
                }
            }

        }
        catch ( EntityNotFoundException e )
        {
            e.printStackTrace();
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
        return Query2Description.instance;
    }
}
