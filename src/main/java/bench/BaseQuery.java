package bench;

import bench.queries.Measurement;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

public abstract class BaseQuery
{
    public static final String[] NO_HEADER = new String[0];
    public static final long[] NO_DATA = new long[0];

    public void runQuery( GraphDatabaseService graphDb, Measurement measurement, long[] inputData )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            ReadOperations readOperations = ((GraphDatabaseAPI) graphDb).getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class )
                    .get().readOperations();

            long start = System.currentTimeMillis();
            doRunQuery( readOperations, measurement, inputData );
            measurement.countSuccess( System.currentTimeMillis() - start );

            tx.success();
        }
        catch ( Exception e )
        {
        }
    }

    public abstract String[] inputDataHeader();

    protected abstract String query();

    protected abstract void doRunQuery( ReadOperations operations, Measurement measurement, long[] inputData );

    protected PrimitiveLongIterator getNodeFromIndexLookup( ReadOperations operations,
                                                          int labelId, int propertyId, Object value )
            throws IndexNotFoundKernelException
    {
        return operations.nodesGetFromIndexLookup( new IndexDescriptor( labelId, propertyId ), value );
    }
}
