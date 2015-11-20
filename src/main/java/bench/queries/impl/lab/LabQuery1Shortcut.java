package bench.queries.impl.lab;

import bench.laboratory.LabEnvironmentGenerator;
import bench.queries.QueryDescription;
import bench.queries.impl.description.LabQuery1Description;
import index.SCIndexDescription;
import index.SCResultVisitor;
import index.Seeker;
import index.btree.RangePredicate;
import index.btree.RangeSeeker;

import org.neo4j.graphdb.Direction;

public class LabQuery1Shortcut extends LabQueryShortcut
{
    private final int percentageOfRange;
    public SCIndexDescription indexDescription = new SCIndexDescription( "Person", "Comment",
            "CREATED", Direction.OUTGOING, null, "date" );

    private int lowerBoundary;
    private int upperBoundary;

    public LabQuery1Shortcut( int percentageOfRange )
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
    protected Seeker seeker( long start )
    {
        return new RangeSeeker(
                RangePredicate.greaterOrEqual( start, lowerBoundary ),
                RangePredicate.lower( start, upperBoundary ) );
    }

    @Override
    public QueryDescription queryDescription()
    {
        return LabQuery1Description.instance( percentageOfRange );
    }

    @Override
    protected SCResultVisitor getVisitor()
    {
        return new SCResultVisitor.CountingResultVisitor();
    }
}
