package bench.queries.impl.ldbc;

import bench.queries.QueryDescription;
import bench.queries.impl.description.Query3Description;
import bench.queries.impl.framework.QueryKernelWithPropertyOnRelationship;
import bench.util.SingleEntryPrimitiveLongIterator;
import index.SCResult;

import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class Query3Kernel extends QueryKernelWithPropertyOnRelationship
{
    public Query3Kernel()
    {
        super();
    }

    @Override
    protected boolean filterOnRelationshipProperty( long prop )
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
        return "Post";
    }

    @Override
    protected String relType()
    {
        return "LIKES_POST";
    }

    @Override
    protected Direction direction()
    {
        return Direction.OUTGOING;
    }

    @Override
    protected String propKey()
    {
        return "creationDate";
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
    protected void massageRawResult( List<SCResult> resultList )
    {
        resultList.sort( ( o1, o2) -> -Long.compare( o1.getKey().getProp(), o2.getKey().getProp() ) );
    }

    @Override
    public QueryDescription queryDescription()
    {
        return Query3Description.instance;
    }
}
