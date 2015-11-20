package bench.queries.impl.framework;

import bench.Measurement;
import bench.QueryType;
import bench.queries.Query;
import index.SCIndexDescription;
import index.SCIndexProvider;
import index.SCResult;
import index.SCResultVisitor;

import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;

public abstract class QueryKernel extends Query
{
    protected abstract String firstLabel();

    protected abstract String secondLabel();

    protected abstract String relType();

    protected abstract Direction direction();

    protected abstract String propKey();

    protected abstract PrimitiveLongIterator startingPoints(
            ReadOperations operations, long[] inputData, int firstLabel ) throws EntityNotFoundException;

    protected abstract void expandFromStart( ReadOperations operations, Measurement measurement, long[] inputData,
            long startPoint,
            int relType, int secondLabel,
            int propKey, SCResultVisitor visitor );

    @Override
    protected long doRunQuery(
            ReadOperations operations, Measurement measurement, long[] inputData ) throws EntityNotFoundException
    {
        int firstLabel = operations.labelGetForName( firstLabel() );
        int relType = operations.relationshipTypeGetForName( relType() );
        int secondLabel = operations.labelGetForName( secondLabel() );
        int propKey = operations.propertyKeyGetForName( propKey() );

        SCResultVisitor visitor = getVisitor();

        PrimitiveLongIterator startingPoints = startingPoints( operations, inputData, firstLabel );

        while ( startingPoints.hasNext() )
        {
            long startPoint = startingPoints.next();

            expandFromStart( operations, measurement, inputData, startPoint, relType, secondLabel, propKey,
                    visitor );
        }

        visitor.massageRawResult();
        visitor.limit();
        return visitor.rowCount();
    }

    protected List<SCResult> limit( List<SCResult> resultList )
    {
        // Do nothing here as default
        return resultList;
    }

    protected PrimitiveLongIterator getNodeFromIndexLookup( ReadOperations operations,
                                                          int labelId, int propertyId, Object value )
            throws IndexNotFoundKernelException
    {
        return operations.nodesGetFromIndexSeek( new IndexDescriptor( labelId, propertyId ), value );
    }

    @Override
    public QueryType type()
    {
        return QueryType.KERNEL;
    }

    @Override
    public void setIndexProvider( SCIndexProvider indexes )
    {
        // Do nothing, kernel queries don't use an SCIndex
    }

    @Override
    public SCIndexDescription indexDescription()
    {
        return null;
    }
}
