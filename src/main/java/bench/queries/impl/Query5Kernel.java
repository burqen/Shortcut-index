package bench.queries.impl;

import bench.queries.framework.QueryDescription;
import bench.queries.framework.QueryKernelWithPropertyOnRelationship;
import bench.queries.impl.description.Query5Description;
import bench.util.Config;
import bench.util.SingleEntryPrimitiveLongIterator;
import index.logical.TResult;

import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class Query5Kernel extends QueryKernelWithPropertyOnRelationship
{
    @Override
    protected boolean filterOnRelationshipProperty( long prop )
    {
        return prop < 2010l;
    }

    @Override
    protected String firstLabel()
    {
        return "Company";
    }

    @Override
    protected String secondLabel()
    {
        return "Person";
    }

    @Override
    protected String relType()
    {
        return "WORKS_AT";
    }

    @Override
    protected Direction direction()
    {
        return Direction.INCOMING;
    }

    @Override
    protected String propKey()
    {
        return "workFrom";
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
        return Query5Description.instance;
    }
}
