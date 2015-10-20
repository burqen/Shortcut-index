package bench.queries.impl.ldbc;

import bench.queries.QueryDescription;
import bench.queries.impl.framework.QueryKernelWithPropertyOnRelationship;
import bench.queries.impl.description.Query4Description;
import bench.util.SingleEntryPrimitiveLongIterator;
import index.SCResult;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class Query4Kernel extends QueryKernelWithPropertyOnRelationship
{
    @Override
    protected boolean filterOnRelationshipProperty( long prop )
    {
        return prop != 2010;
    }

    @Override
    protected String firstLabel()
    {
        return "University";
    }

    @Override
    protected String secondLabel()
    {
        return "Person";
    }

    @Override
    protected String relType()
    {
        return "STUDY_AT";
    }

    @Override
    protected Direction direction()
    {
        return Direction.INCOMING;
    }

    @Override
    protected String propKey()
    {
        return "classYear";
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
        return Query4Description.instance;
    }
}
