package bench.queries.impl.description;

import bench.queries.QueryDescription;
import bench.util.Config;

public class Query6Description extends QueryDescription
{
    @Override
    public String queryName()
    {
        return "Query6";
    }

    @Override
    public String cypher()
    {
        return "// QUERY 6 - RANGE <= prop <\n" +
               "// Posts posted to a forum in a time interval\n" +
               "MATCH (f:Forum {id:{1}}) -[r:CONTAINER_OF]-> (p:Post)\n" +
               "WHERE 2011 <= p.creationDate AND p.creationDate < 2012\n" +
               "RETURN id(f), id(r), id(p), p.creationDate;\n";
    }

    @Override
    public String[] inputDataHeader()
    {
        return new String[]{ "Forum" };
    }

    @Override
    public String inputFile()
    {
        return Config.QUERY6_PARAMETERS;
    }

    public static QueryDescription instance = new Query6Description();
}
