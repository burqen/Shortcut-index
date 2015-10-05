package bench.queries.impl.description;

import bench.queries.QueryDescription;
import bench.util.Config;

public class Query5Description extends QueryDescription
{
    @Override
    public String queryName()
    {
        return "Query5";
    }

    @Override
    public String[] inputDataHeader()
    {
        return new String[]{ "Company" };
    }

    @Override
    public String cypher()
    {
        return "// QUERY 5 - RANGE prop <\n" +
               "// Employees since before 2010\n" +
               "MATCH (c:Company {id:{1}}) <-[r:WORKS_AT]- (p:Person)\n" +
               "WHERE r.workFrom < 2010\n" +
               "RETURN id(c), id(p), r.workFrom\n";
    }

    @Override
    public String inputFile()
    {
        return Config.QUERY5_PARAMETERS;
    }

    public static QueryDescription instance = new Query5Description();
}
