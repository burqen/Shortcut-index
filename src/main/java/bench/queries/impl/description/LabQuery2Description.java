package bench.queries.impl.description;

import bench.queries.QueryDescription;
import bench.util.Config;

public class LabQuery2Description extends LABQueryDescription
{
    private final int limit;

    public LabQuery2Description( int limit )
    {
        this.limit = limit;
    }
    @Override
    public String queryName()
    {
        return String.format( "Lab Query2 limit %d", limit );
    }

    @Override
    public String cypher()
    {
        return "// " + queryName() + "\n" +
               "// Order by date and limit result count\n" +
               "MATCH (p:Person)-[r:CREATED]->(c:Comment)\n" +
               "WHERE id(p) = {1}\n" +
               "RETURN id(p), id(r), id(c), c.date" +
               "ORDER BY c.date ASC" +
               "LIMIT {3}";
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

    public static QueryDescription instance( int limit )
    {
        return new LabQuery2Description( limit );
    }
}
