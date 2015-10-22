package bench.queries.impl.ldbc;

import bench.queries.QueryDescription;
import bench.queries.impl.description.Query6Description;
import bench.queries.impl.framework.QueryKernelWithPropertyOnNode;
import bench.util.SingleEntryPrimitiveLongIterator;
import index.SCResult;

import java.util.Calendar;
import java.util.GregorianCalendar;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class Query6Kernel extends QueryKernelWithPropertyOnNode
{
    private long lowerBoundary;
    private long upperBoundary;

    public Query6Kernel()
    {
        Calendar cal = new GregorianCalendar();
        cal.set( 2011, Calendar.JANUARY, 1 );
        lowerBoundary = cal.getTimeInMillis();
        cal.set( 2012, Calendar.JANUARY, 1 );
        upperBoundary = cal.getTimeInMillis();
    }

    @Override
    protected boolean filterOnNodeProperty( long prop )
    {
        return prop < lowerBoundary || upperBoundary <= prop;
    }

    @Override
    protected String firstLabel()
    {
        return "Forum";
    }

    @Override
    protected String secondLabel()
    {
        return "Post";
    }

    @Override
    protected String relType()
    {
        return "CONTAINER_OF";
    }

    @Override
    protected Direction direction()
    {
        return Direction.OUTGOING;
    }

    @Override
    protected String propKey()
    {
        return "creationDate";
    }

    @Override
    protected PrimitiveLongIterator startingPoints( ReadOperations operations, long[] inputData, int firstLabel )
            throws EntityNotFoundException
    {
        if ( operations.nodeHasLabel( inputData[0], firstLabel ) )
        {
            return new SingleEntryPrimitiveLongIterator( inputData[0] );
        }
        else
        {
            throw new IllegalArgumentException(
                    "Node[" + inputData[0] + "] did not have label " + firstLabel() + " as expected. " +
                    "Use correct input file." );
        }
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
