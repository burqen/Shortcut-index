package bench.queries.framework;

import bench.QueryType;
import bench.Measurement;
import bench.queries.Query;
import index.ShortcutIndexProvider;
import index.legacy.TResult;

import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
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
    public void runQuery( ThreadToStatementContextBridge threadToStatementContextBridge, GraphDatabaseService graphDb,
            Measurement measurement, long[] inputData )
    {
        long start = System.nanoTime();
        List<TResult> resultList;
        try ( Transaction tx = graphDb.beginTx() )
        {
            ReadOperations readOperations = threadToStatementContextBridge.get().readOperations();

            resultList = doRunQuery( readOperations, measurement, inputData );

            tx.success();
        }
        measurement.queryFinished( ( System.nanoTime() - start ) / 1000, resultList.size() );

        reportResult( resultList );
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
