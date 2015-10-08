package bench.queries.impl;

import bench.Measurement;
import bench.queries.QueryDescription;
import bench.queries.framework.QueryShortcut;
import bench.queries.impl.description.Query6Description;
import index.SCIndexDescription;
import index.legacy.LegacySCIndex;
import index.legacy.RangeSeeker;
import index.ShortcutIndexProvider;
import index.legacy.TResult;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class Query6Shortcut extends QueryShortcut
{
    public static SCIndexDescription indexDescription = new SCIndexDescription( "Forum", "Post",
            "CONTAINER_OF", Direction.OUTGOING, null, "creationDate" );

    private long lowerBoundary;
    private long upperBoundary;

    public Query6Shortcut( ShortcutIndexProvider indexes )
    {
        super( indexes );
        Calendar cal = new GregorianCalendar();
        cal.set( 2011, Calendar.JANUARY, 1 );
        lowerBoundary = cal.getTimeInMillis();
        cal.set( 2012, Calendar.JANUARY, 1 );
        upperBoundary = cal.getTimeInMillis();
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

            LegacySCIndex index = indexes.get( indexDescription );

            index.seek( new RangeSeeker( start, lowerBoundary, upperBoundary ), indexSeekResult );

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
        return Query6Description.instance;
    }
}
