package bench.queries.impl.description;

import bench.queries.QueryDescription;
import bench.util.Config;

public class Query4Description extends LDBCQueryDescription
{
    @Override
    public String queryName()
    {
        return "LDBC Query4";
    }

    @Override
    public String[] inputDataHeader()
    {
        return new String[]{ "University" };
    }

    @Override
    public String cypher()
    {
        return "// QUERY 4 - EXACT MATCH\n" +
               "// Number of students studying at university 2010\n" +
               "MATCH (u:University {id:{1}}) <-[r:STUDY_AT]- (p:Person)\n" +
               "WHERE r.classYear = 2010\n" +
               "RETURN id(u), id(p), r.classYear;\n";
    }

    @Override
    public String inputFile()
    {
        return Config.QUERY4_PARAMETERS;
    }

    public static final QueryDescription instance = new Query4Description();
}
