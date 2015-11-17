package bench.queries.impl.lab;

import bench.laboratory.LabEnvironmentGenerator;
import bench.queries.QueryDescription;
import bench.queries.impl.description.LabQuery2Description;
import index.SCIndexDescription;
import index.Seeker;
import index.btree.CountPredicate;
import index.btree.RangePredicate;
import index.btree.RangeSeeker;

import org.neo4j.graphdb.Direction;

public class LabQuery2Shortcut extends LabQueryShortcut
{
    private final int limit;
    public SCIndexDescription indexDescription = new SCIndexDescription( "Person", "Comment",
            "CREATED", Direction.OUTGOING, null, "date" );

    private int lowerBoundary;
    private int upperBoundary;

    public LabQuery2Shortcut( int limit )
    {
        this.limit = limit;
        if ( limit < 1 )
        {
            throw new IllegalArgumentException( "Limit is less than one: " + limit );
        }
        lowerBoundary = 0;
        upperBoundary = LabEnvironmentGenerator.RANGE_MAX;
    }

    @Override
    protected Seeker seeker( long start )
    {
        return new RangeSeeker(
                RangePredicate.greaterOrEqual( start, lowerBoundary ),
                RangePredicate.lower( start, upperBoundary ), CountPredicate.max( limit ), false );
    }

    @Override
    public QueryDescription queryDescription()
    {
        return LabQuery2Description.instance( limit );
    }
}
