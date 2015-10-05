package bench.queries.impl.description;

import bench.queries.framework.QueryDescription;
import bench.util.Config;

public class Query1Description extends QueryDescription
{
    @Override
    public String queryName()
    {
        return "Query1";
    }

    @Override
    public String cypher()
    {
        return "// QUERY 1 - SCAN\n" +
               "// All comments written by all persons\n" +
               "MATCH (p:Person) <-[r:COMMENT_HAS_CREATOR]- (c:Comment)\n" +
               "RETURN id(p), id(r), id(c), c.creationDate;\n";
    }

    @Override
    public String[] inputDataHeader()
    {
        return Config.NO_HEADER;
    }

    @Override
    public String inputFile()
    {
        return Config.NO_INPUT;
    }


    public static QueryDescription instance = new Query1Description();
}
