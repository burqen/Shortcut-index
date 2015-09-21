package bench.queries;

import bench.BaseQuery;

import org.neo4j.kernel.api.ReadOperations;

public abstract class Query1 extends BaseQuery
{
    private final String[] inputDataHeader = NO_HEADER;

    @Override
    public String query()
    {
        return "// QUERY 1 - SCAN" +
               "// All comments written by all persons\n" +
               "MATCH (p:Person) <-[r:COMMENT_HAS_CREATOR]- (c:Comment)\n" +
               "RETURN id(p), id(r), id(c), c.creationDate;";
    }

    @Override
    protected void doRunQuery( ReadOperations operations, Measurement measurement, long[] inputData )
    {

    }

    @Override
    public String[] inputDataHeader()
    {
        return inputDataHeader;
    }
}
