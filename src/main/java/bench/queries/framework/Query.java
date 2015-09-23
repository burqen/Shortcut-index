package bench.queries.framework;

import org.neo4j.graphdb.GraphDatabaseService;

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

    public abstract void runQuery( GraphDatabaseService graphDb, Measurement measurement, long[] inputData );

}
