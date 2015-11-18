package bench.queries.impl.lab;

import bench.queries.QueryDescription;
import bench.queries.impl.description.LabQuery3Description;
import index.SCIndexDescription;
import index.Seeker;
import index.btree.RangePredicate;
import index.btree.RangeSeeker;

import org.neo4j.graphdb.Direction;

public class LabQuery3Shortcut extends LabQueryShortcut
{
    public SCIndexDescription indexDescription = new SCIndexDescription( "Person", "Comment",
            "CREATED", Direction.OUTGOING, null, "date" );

    @Override
    protected Seeker seeker( long start )
    {
        return new RangeSeeker( RangePredicate.noLimit( start ), RangePredicate.noLimit( start ), false );
    }

    @Override
    public QueryDescription queryDescription()
    {
        return LabQuery3Description.INSTANCE;
    }
}
