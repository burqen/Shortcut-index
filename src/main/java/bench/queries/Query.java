package bench.queries;

import bench.Environment;
import bench.Measurement;
import bench.QueryType;
import index.SCIndexDescription;
import index.SCIndexProvider;
import index.SCResult;

import java.io.IOException;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

public abstract class Query
{
    public String[] inputDataHeader()
    {
        return queryDescription().inputDataHeader();
    }

    public String cypher()
    {
        return queryDescription().cypher();
    }

    public String inputFile()
    {
        return queryDescription().inputFile();
    }

    public Environment environment()
    {
        return queryDescription().environment();
    }

    public abstract QueryDescription queryDescription();

    public abstract SCIndexDescription indexDescription();

    public void runQuery( ThreadToStatementContextBridge threadToStatementContextBridge, GraphDatabaseService graphDb,
            Measurement measurement, long[] inputData ) throws IOException, EntityNotFoundException
    {
        long start = System.nanoTime();
        List<SCResult> resultList;
        try ( Transaction tx = graphDb.beginTx() )
        {
            ReadOperations readOperations = threadToStatementContextBridge.get().readOperations();

            resultList = doRunQuery( readOperations, measurement, inputData );

            tx.success();
        }
        long elapsedTime = (System.nanoTime() - start) / 1000;
        measurement.queryFinished( elapsedTime, resultList.size() );

        reportResult( resultList );
    }

    protected void reportResult( List<SCResult> resultList )
    {
        // Do nothing here as default
    }

    protected abstract List<SCResult> doRunQuery(
            ReadOperations operations, Measurement measurement, long[] inputData )
            throws IOException, EntityNotFoundException;

    public abstract QueryType type();

    public abstract void setIndexProvider( SCIndexProvider indexes );
}
