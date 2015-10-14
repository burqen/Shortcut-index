package bench.queries.impl;

import bench.Measurement;
import bench.queries.QueryDescription;
import bench.queries.framework.QueryShortcut;
import bench.queries.impl.description.Query6Description;
import index.SCIndex;
import index.SCIndexDescription;
import index.SCIndexProvider;
import index.SCResult;
import index.btree.RangePredicate;
import index.btree.RangeSeeker;

import java.io.IOException;
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

    public Query6Shortcut( SCIndexProvider indexes )
    {
        super( indexes );
        Calendar cal = new GregorianCalendar();
        cal.set( 2011, Calendar.JANUARY, 1 );
        lowerBoundary = cal.getTimeInMillis();
        cal.set( 2012, Calendar.JANUARY, 1 );
        upperBoundary = cal.getTimeInMillis();
    }

    @Override
    protected List<SCResult> doRunQuery( ReadOperations operations, Measurement measurement, long[] inputData )
            throws IOException
    {
        List<SCResult> indexSeekResult = new ArrayList<>();
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

            SCIndex index = indexes.get( indexDescription );


            index.seek( new RangeSeeker( RangePredicate.greaterOrEqual( start, lowerBoundary ),
                    RangePredicate.lower( start, upperBoundary ) ), indexSeekResult );
            Iterator<SCResult> resultIterator = indexSeekResult.iterator();
            while ( resultIterator.hasNext() )
            {
                SCResult result = resultIterator.next();
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
    protected boolean filterResultRow( SCResult resultRow )
    {
        return false;
    }

    @Override
    public QueryDescription queryDescription()
    {
        return Query6Description.instance;
    }
}
