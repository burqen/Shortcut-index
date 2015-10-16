package bench.queries.impl.description;

import bench.queries.QueryDescription;
import bench.util.Config;

public class LABQuery1Description extends LABQueryDescription
{
    @Override
    public String queryName()
    {
        return "Lab Query1";
    }

    @Override
    public String cypher()
    {
        return "// LAB Query 1\n" +
               "// Range covers % of total range -> % of neighbourhood because of uniform distribution\n" +
               "// 1: 100%, 2: 75%, 3: 50%, 4: 25% 5: 1%\n" +
               "MATCH (p:Person)-[r:CREATED]->(c:Comment)\n" +
               "WHERE id(p) = {1} AND {2} <= c.date < {3}\n" +
               "RETURN id(p), id(r), id(c), c.date";
    }

    @Override
    public String[] inputDataHeader()
    {
        return new String[]{ "Person" };
    }

    @Override
    public String inputFile()
    {
        return Config.LAB_QUERY1_PARAMETERS;
    }

    public static QueryDescription instance = new LABQuery1Description();
}
