package bench.queries.impl.lab;

import bench.Measurement;
import bench.queries.impl.description.LABQuery1Description;
import bench.queries.QueryDescription;
import bench.queries.framework.QueryShortcut;
import index.SCIndex;
import index.SCIndexDescription;
import index.SCResult;
import index.btree.RangePredicate;
import index.btree.RangeSeeker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class LabQuery1Shortcut extends QueryShortcut
{
    public SCIndexDescription indexDescription = new SCIndexDescription( "Person", "Comment",
            "CREATED", Direction.OUTGOING, null, "date" );

    private long lowerBoundary;
    private long upperBoundary;

    public LabQuery1Shortcut( int percentageOfRange )
    {
        if ( percentageOfRange < 1 || percentageOfRange > 100 )
        {
            throw new IllegalArgumentException( "Percentage is outside range 1-100: " + percentageOfRange );
        }
        // CONTINUE HERE!
        lowerBoundary = 0; // TODO: BETTER VALUE NEEDED
        upperBoundary = 10000000; // TODO: BETTER VALUE NEEDED
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


            index.seek( new RangeSeeker(
                    RangePredicate.greaterOrEqual( start, lowerBoundary ),
                    RangePredicate.lower( start, upperBoundary ) ),
                    indexSeekResult );

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
    public SCIndexDescription indexDescription()
    {
        return indexDescription;
    }

    @Override
    public QueryDescription queryDescription()
    {
        return LABQuery1Description.instance;
    }
}
