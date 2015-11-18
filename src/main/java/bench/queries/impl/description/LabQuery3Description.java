package bench.queries.impl.description;

import bench.queries.QueryDescription;
import bench.util.Config;

public class LabQuery3Description extends LABQueryDescription
{
    @Override
    public String queryName()
    {
        return "Lab Query3";
    }

    @Override
    public String cypher()
    {
        return "// " + queryName() + "\n" +
               "// Order by date and limit result count\n" +
               "MATCH (p:Person)-[r:CREATED]->(c:Comment)\n" +
               "WHERE id(p) = {1}\n" +
               "RETURN id(p), id(r), id(c), c.date" +
               "ORDER BY c.date ASC";
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

    public static final QueryDescription INSTANCE = new LabQuery3Description();
}
