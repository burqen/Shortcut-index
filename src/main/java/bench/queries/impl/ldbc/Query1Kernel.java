package bench.queries.impl.ldbc;

import bench.queries.QueryDescription;
import bench.queries.impl.description.Query1Description;
import bench.queries.impl.framework.QueryKernelWithPropertyOnNode;
import index.SCResultVisitor;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

public class Query1Kernel extends QueryKernelWithPropertyOnNode
{
    public Query1Kernel()
    {
        super();
    }

    @Override
    protected boolean filterOnNodeProperty( long prop )
    {
        return false;
    }

    @Override
    protected PrimitiveLongIterator startingPoints( ReadOperations operations, long[] inputData, int firstLabel )
    {
        return operations.nodesGetForLabel( firstLabel );
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
        return "COMMENT_HAS_CREATOR";
    }

    @Override
    protected Direction direction()
    {
        return Direction.INCOMING;
    }

    @Override
    protected String propKey()
    {
        return "creationDate";
    }

    @Override
    public QueryDescription queryDescription()
    {
        return Query1Description.instance;
    }

    @Override
    protected SCResultVisitor getVisitor()
    {
        return new SCResultVisitor.CountingResultVisitor();
    }
}
