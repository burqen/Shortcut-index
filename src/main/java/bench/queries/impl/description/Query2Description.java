package bench.queries.impl.description;

import bench.queries.QueryDescription;
import bench.util.Config;

public class Query2Description extends LDBCQueryDescription
{
    @Override
    public String queryName()
    {
        return "LDBC Query2";
    }

    @Override
    public String[] inputDataHeader()
    {
        return new String[]{ "Person" };
    }

    @Override
    public String cypher()
    {
        return "// QUERY 2 - SEEK\n" +
               "// All comments written by person\n" +
               "MATCH (p:Person {id:{1}}) <-[r:COMMENT_HAS_CREATOR]- (c:Comment)\n" +
               "RETURN id(p), id(r), id(c), c.creationDate\n";
    }

    @Override
    public String inputFile()
    {
        return Config.QUERY2_PARAMETERS;
    }

    public static QueryDescription instance = new Query2Description();
}
