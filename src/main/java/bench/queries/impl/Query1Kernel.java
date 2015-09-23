package bench.queries.impl;

import bench.queries.framework.QueryDescription;
import bench.queries.framework.QueryKernelWithPropertyOnNode;
import bench.queries.impl.description.Query1Description;
import bench.util.Config;
import index.logical.TResult;

import java.util.List;

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
    protected boolean filterResultRow( TResult resultRow )
    {
        return false;
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
}
