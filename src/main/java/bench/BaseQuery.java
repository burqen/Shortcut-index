package bench;

import bench.queries.Measurement;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.OperationsFacade;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

public abstract class BaseQuery
{
    public void runQuery( GraphDatabaseService graphDb, BenchLogger logger )
    {
        logger.start( query() );
        try ( Transaction tx = graphDb.beginTx() )
        {
            ReadOperations readOperations = ((GraphDatabaseAPI) graphDb).getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class )
                    .get().readOperations();

            doRunQuery( readOperations, logger.measurement() );

            tx.success();
        }
        logger.end();
    }

    protected abstract String query();

    protected abstract void doRunQuery( ReadOperations operations, Measurement measurement );

    protected PrimitiveLongIterator getNodeFromIndexLookup( ReadOperations operations,
                                                          int labelId, int propertyId, Object value )
            throws IndexNotFoundKernelException
    {
        return operations.nodesGetFromIndexLookup( new IndexDescriptor( labelId, propertyId ), value );
    }
}
