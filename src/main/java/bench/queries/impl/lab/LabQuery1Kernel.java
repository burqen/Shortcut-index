package bench.queries.impl.lab;

import bench.laboratory.LabEnvironmentGenerator;
import bench.queries.impl.description.LABQuery1Description;
import bench.queries.QueryDescription;
import bench.queries.framework.QueryKernelWithPropertyOnNode;
import bench.util.SingleEntryPrimitiveLongIterator;
import index.SCResult;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class LabQuery1Kernel extends QueryKernelWithPropertyOnNode
{
    private final int percentageOfRange;
    private int lowerBoundary;
    private int upperBoundary;

    public LabQuery1Kernel( int percentageOfRange )
    {
        this.percentageOfRange = percentageOfRange;
        if ( percentageOfRange < 1 || percentageOfRange > 100 )
        {
            throw new IllegalArgumentException( "Percentage is outside range 1-100: " + percentageOfRange );
        }
        lowerBoundary = 0;
        upperBoundary = percentageOfRange * LabEnvironmentGenerator.RANGE_MAX / 100;
    }


    @Override
    protected boolean filterOnNodeProperty( long prop )
    {
        return prop < lowerBoundary || prop >= upperBoundary;
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
        return "CREATED";
    }

    @Override
    protected Direction direction()
    {
        return Direction.OUTGOING;
    }

    @Override
    protected String propKey()
    {
        return "date";
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
        return LABQuery1Description.instance( percentageOfRange );
    }
}
