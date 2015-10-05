package bench.queries.impl.description;

import bench.queries.QueryDescription;
import bench.util.Config;

public class Query3Description extends QueryDescription
{
    @Override
    public String queryName()
    {
        return "Query3";
    }

    @Override
    public String[] inputDataHeader()
    {
        return new String[]{ "Person" };
    }

    @Override
    public String cypher()
    {
        return "// QUERY 3 - ORDER BY\n" +
               "// Most recently liked post by person\n" +
               "MATCH (p:Person {id:{1}}) -[r:LIKES_POST]-> (o:Post)\n" +
               "RETURN id(p), id(r), id(o), r.creationDate\n" +
               "ORDER BY r.creationDate DESC;\n";
    }

    @Override
    public String inputFile()
    {
        return Config.QUERY3_PARAMETERS;
    }

    public static QueryDescription instance = new Query3Description();
}
