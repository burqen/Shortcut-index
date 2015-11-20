package bench.queries.impl.ldbc;

import bench.Measurement;
import bench.queries.QueryDescription;
import bench.queries.impl.description.Query6Description;
import bench.queries.impl.framework.QueryShortcut;
import index.SCIndex;
import index.SCIndexDescription;
import index.SCResultVisitor;
import index.btree.RangePredicate;
import index.btree.RangeSeeker;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class Query6Shortcut extends QueryShortcut
{
    public SCIndexDescription indexDescription = new SCIndexDescription( "Forum", "Post",
            "CONTAINER_OF", Direction.OUTGOING, null, "creationDate" );

    private long lowerBoundary;
    private long upperBoundary;

    public Query6Shortcut()
    {
        Calendar cal = new GregorianCalendar();
        cal.set( 2011, Calendar.JANUARY, 1 );
        lowerBoundary = cal.getTimeInMillis();
        cal.set( 2012, Calendar.JANUARY, 1 );
        upperBoundary = cal.getTimeInMillis();
    }

    @Override
    protected long doRunQuery( ReadOperations operations, Measurement measurement, long[] inputData )
            throws IOException
    {
        SCResultVisitor visitor = getVisitor();
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
                    RangePredicate.lower( start, upperBoundary ) ), visitor );
        }
        catch ( EntityNotFoundException e )
        {
            e.printStackTrace();
        }
        visitor.massageRawResult();
        visitor.limit();
        return visitor.rowCount();
    }

    @Override
    public SCIndexDescription indexDescription()
    {
        return indexDescription;
    }

    @Override
    protected SCResultVisitor getVisitor()
    {
        return new SCResultVisitor.CountingResultVisitor();
    }

    @Override
    public QueryDescription queryDescription()
    {
        return Query6Description.instance;
    }
}
