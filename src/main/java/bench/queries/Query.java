package bench.queries;

import bench.Environment;
import bench.QueryType;
import bench.Measurement;
import index.SCIndexDescription;
import index.SCIndexProvider;

import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
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

    public abstract void runQuery( ThreadToStatementContextBridge threadToStatementContextBridge,
            GraphDatabaseService graphDb, Measurement measurement, long[] inputData )
            throws EntityNotFoundException, IOException;

    public abstract QueryType type();

    public abstract void setIndexProvider( SCIndexProvider indexes );
}
