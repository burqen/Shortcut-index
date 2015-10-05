package bench.queries.framework;

import bench.QueryType;
import bench.Measurement;
import bench.queries.Query;
import index.logical.ShortcutIndexProvider;
import index.logical.TResult;

import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

public abstract class QueryShortcut extends Query
{
    protected final ShortcutIndexProvider indexes;

    public QueryShortcut( ShortcutIndexProvider indexes )
    {
        this.indexes = indexes;
    }

    @Override
    public void runQuery( GraphDatabaseService graphDb, Measurement measurement, long[] inputData )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            ReadOperations readOperations = ((GraphDatabaseAPI) graphDb).getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class )
                    .get().readOperations();

            long start = System.nanoTime();
            List<TResult> resultList = doRunQuery( readOperations, measurement, inputData );

            measurement.queryFinished( ( System.nanoTime() - start ) / 1000, resultList.size() );

            reportResult( resultList );

            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    protected void reportResult( List<TResult> resultList )
    {
        // Do nothing here as default
    }

    protected abstract List<TResult> doRunQuery(
            ReadOperations operations, Measurement measurement, long[] inputData );

    protected abstract boolean filterResultRow( TResult resultRow );

    @Override
    public QueryType type()
    {
        return QueryType.SHORTCUT;
    }
}
