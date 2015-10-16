package bench.queries.framework;

import bench.QueryType;
import bench.Measurement;
import bench.queries.Query;
import index.SCIndexDescription;
import index.SCIndexProvider;
import index.SCResult;

import java.io.IOException;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

public abstract class QueryShortcut extends Query
{
    protected SCIndexProvider indexes;

    @Override
    public void runQuery( ThreadToStatementContextBridge threadToStatementContextBridge, GraphDatabaseService graphDb,
            Measurement measurement, long[] inputData ) throws IOException
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

    @Override
    public void setIndexes( SCIndexProvider indexes )
    {
        this.indexes = indexes;
    }

    protected void reportResult( List<SCResult> resultList )
    {
        // Do nothing here as default
    }

    protected abstract List<SCResult> doRunQuery(
            ReadOperations operations, Measurement measurement, long[] inputData ) throws IOException;

    protected abstract boolean filterResultRow( SCResult resultRow );

    @Override
    public QueryType type()
    {
        return QueryType.SHORTCUT;
    }
}
