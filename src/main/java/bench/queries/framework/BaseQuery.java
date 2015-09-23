package bench.queries.framework;

import bench.queries.Measurement;
import index.logical.ShortcutIndexProvider;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

public abstract class BaseQuery
{
    protected final ShortcutIndexProvider indexes;

    public BaseQuery()
    {
        indexes = null;
    }

    public BaseQuery( ShortcutIndexProvider indexes )
    {
        this.indexes = indexes;
    }

    protected abstract String firstLabel();

    protected abstract String secondLabel();

    protected abstract String relType();

    protected abstract Direction direction();

    protected abstract String propKey();

    public abstract String[] inputDataHeader();

    public abstract String query();

    protected abstract PrimitiveLongIterator startingPoints(
            ReadOperations operations, long[] inputData, int firstLabel );

    protected abstract boolean validateRow( long startPoint, long otherNode, long relationship, long prop );

    protected abstract void expandFromStart( ReadOperations operations, Measurement measurement, long[] inputData,
            long startPoint,
            int relType, int secondLabel,
            int propKey );

    protected abstract void doRunQueryWithIndex(
            ReadOperations operations, Measurement measurement, long[] inputData );

    public void runQuery( GraphDatabaseService graphDb, Measurement measurement, long[] inputData )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            ReadOperations readOperations = ((GraphDatabaseAPI) graphDb).getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class )
                    .get().readOperations();

            long start = System.currentTimeMillis();
            if ( indexes == null )
            {
                doRunQueryWithoutIndex( readOperations, measurement, inputData );
            }
            else
            {
                doRunQueryWithIndex( readOperations, measurement, inputData );
            }
            measurement.countSuccess( System.currentTimeMillis() - start );

            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    protected void doRunQueryWithoutIndex(
            ReadOperations operations, Measurement measurement, long[] inputData )
    {
        int firstLabel = operations.labelGetForName( firstLabel() );
        int relType = operations.relationshipTypeGetForName( relType() );
        int secondLabel = operations.labelGetForName( secondLabel() );
        int propKey = operations.propertyKeyGetForName( propKey() );

        PrimitiveLongIterator startingPoints = startingPoints( operations, inputData, firstLabel );

        long prop;

        while ( startingPoints.hasNext() )
        {
            long startPoint = startingPoints.next();

            expandFromStart( operations, measurement, inputData, startPoint, relType, secondLabel, propKey );
        }
    }

    protected PrimitiveLongIterator getNodeFromIndexLookup( ReadOperations operations,
                                                          int labelId, int propertyId, Object value )
            throws IndexNotFoundKernelException
    {
        return operations.nodesGetFromIndexLookup( new IndexDescriptor( labelId, propertyId ), value );
    }

    public abstract String inputFile();
}
