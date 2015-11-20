package bench.queries.impl.ldbc;

import bench.Measurement;
import bench.queries.QueryDescription;
import bench.queries.impl.description.Query1Description;
import bench.queries.impl.framework.QueryShortcut;
import index.SCIndex;
import index.SCIndexDescription;
import index.SCResultVisitor;
import index.btree.util.SeekerFactory;

import java.io.IOException;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

public class Query1Shortcut extends QueryShortcut
{
    private SCIndexDescription indexDescription = new SCIndexDescription( "Person", "Comment",
            "COMMENT_HAS_CREATOR", Direction.INCOMING, null, "creationDate" );

    @Override
    protected long doRunQuery( ReadOperations operations, Measurement measurement, long[] inputData )
            throws IOException
    {
        SCIndex index = indexes.get( indexDescription() );

        SCResultVisitor visitor = getVisitor();
        index.seek( SeekerFactory.scanner(), visitor );

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
        return Query1Description.instance;
    }
}
