package bench.queries;

import bench.QueryType;
import bench.Measurement;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

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

    public abstract QueryDescription queryDescription();

    public abstract void runQuery( GraphDatabaseService graphDb, Measurement measurement, long[] inputData )
            throws EntityNotFoundException;

    public abstract QueryType type();
}