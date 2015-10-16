package bench.queries.framework;

import bench.QueryType;
import bench.Measurement;
import bench.queries.Query;
import index.SCIndexDescription;
import index.SCIndexProvider;
import index.SCResult;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

public abstract class QueryKernel extends Query
{
    protected abstract String firstLabel();

    protected abstract String secondLabel();

    protected abstract String relType();

    protected abstract Direction direction();

    protected abstract String propKey();

    protected abstract PrimitiveLongIterator startingPoints(
            ReadOperations operations, long[] inputData, int firstLabel ) throws EntityNotFoundException;

    protected abstract boolean filterResultRow( SCResult resultRow );

    protected abstract void expandFromStart( ReadOperations operations, Measurement measurement, long[] inputData,
            long startPoint,
            int relType, int secondLabel,
            int propKey, List<SCResult> resultList );


    @Override
    public void runQuery( ThreadToStatementContextBridge threadToStatementContextBridge, GraphDatabaseService graphDb,
            Measurement measurement, long[] inputData )
            throws EntityNotFoundException
    {
        long start = System.nanoTime();
        List<SCResult> resultList;
        try ( Transaction tx = graphDb.beginTx() )
        {
            ReadOperations readOperations = threadToStatementContextBridge.get().readOperations();

            resultList = doRunQuery( readOperations, measurement, inputData );

            tx.success();
        }
        measurement.queryFinished( ( System.nanoTime() - start ) / 1000, resultList.size() );

        reportResult( resultList );
    }

    protected List<SCResult> doRunQuery(
            ReadOperations operations, Measurement measurement, long[] inputData ) throws EntityNotFoundException
    {
        int firstLabel = operations.labelGetForName( firstLabel() );
        int relType = operations.relationshipTypeGetForName( relType() );
        int secondLabel = operations.labelGetForName( secondLabel() );
        int propKey = operations.propertyKeyGetForName( propKey() );

        List<SCResult> resultList = new ArrayList<>();

        PrimitiveLongIterator startingPoints = startingPoints( operations, inputData, firstLabel );

        while ( startingPoints.hasNext() )
        {
            long startPoint = startingPoints.next();

            expandFromStart( operations, measurement, inputData, startPoint, relType, secondLabel, propKey,
                    resultList );
        }

        massageRawResult( resultList );
        return resultList;
    }

    protected void reportResult( List<SCResult> resultList )
    {
        // Do nothing here as default
    }

    protected void massageRawResult( List<SCResult> resultList )
    {
        // Do nothing here as default
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
    public void setIndexes( SCIndexProvider indexes )
    {
        // Do nothing, kernel queries don't use an SCIndex
    }

    @Override
    public SCIndexDescription indexDescription()
    {
        return null;
    }
}
