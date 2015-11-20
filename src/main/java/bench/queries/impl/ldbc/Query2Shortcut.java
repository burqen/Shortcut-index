package bench.queries.impl.ldbc;

import bench.Measurement;
import bench.queries.QueryDescription;
import bench.queries.impl.description.Query2Description;
import bench.queries.impl.framework.QueryShortcut;
import index.SCIndex;
import index.SCIndexDescription;
import index.SCResultVisitor;
import index.btree.RangePredicate;
import index.btree.RangeSeeker;

import java.io.IOException;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class Query2Shortcut extends QueryShortcut
{
    public SCIndexDescription indexDescription = new SCIndexDescription( "Person", "Comment",
            "COMMENT_HAS_CREATOR", Direction.INCOMING, null, "creationDate" );

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

            index.seek( new RangeSeeker( RangePredicate.noLimit( start ), RangePredicate.noLimit( start ) ),
                    visitor );
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
        return Query2Description.instance;
    }
}
