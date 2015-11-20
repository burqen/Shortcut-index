package bench.queries;

import bench.Environment;
import bench.Measurement;
import bench.QueryType;
import index.SCIndexDescription;
import index.SCIndexProvider;
import index.SCResultVisitor;

import java.io.IOException;

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
        long rowCount;
        try ( Transaction tx = graphDb.beginTx() )
        {
            ReadOperations readOperations = threadToStatementContextBridge.get().readOperations();

            rowCount = doRunQuery( readOperations, measurement, inputData );

            tx.success();
        }
        measurement.queryFinished( ( System.nanoTime() - start ) / 1000, rowCount );
    }

    protected abstract SCResultVisitor getVisitor();

    protected abstract long doRunQuery(
            ReadOperations operations, Measurement measurement, long[] inputData )
            throws IOException, EntityNotFoundException;

    public abstract QueryType type();

    public abstract void setIndexProvider( SCIndexProvider indexes );
}
