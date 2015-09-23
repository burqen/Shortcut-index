package bench.queries.impl;

import bench.queries.framework.QueryDescription;
import bench.queries.framework.QueryKernelWithPropertyOnNode;
import bench.queries.impl.description.Query6Description;
import index.logical.TResult;

import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class Query6Kernel extends QueryKernelWithPropertyOnNode
{
    // TODO: IMPLEMENT THIS CLASS
    @Override
    protected boolean filterOnNodeProperty( long prop )
    {
        return false;
    }

    @Override
    protected String firstLabel()
    {
        return null;
    }

    @Override
    protected String secondLabel()
    {
        return null;
    }

    @Override
    protected String relType()
    {
        return null;
    }

    @Override
    protected Direction direction()
    {
        return null;
    }

    @Override
    protected String propKey()
    {
        return null;
    }

    @Override
    protected PrimitiveLongIterator startingPoints( ReadOperations operations, long[] inputData, int firstLabel )
            throws EntityNotFoundException
    {
        return null;
    }

    @Override
    protected boolean filterResultRow( TResult resultRow )
    {
        return false;
    }

    @Override
    protected void massageRawResult( List<TResult> resultList )
    {

    }

    @Override
    public QueryDescription queryDescription()
    {
        return Query6Description.instance;
    }
}
