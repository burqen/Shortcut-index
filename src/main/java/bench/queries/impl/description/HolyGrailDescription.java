package bench.queries.impl.description;

import bench.queries.QueryDescription;
import bench.util.Config;

public class HolyGrailDescription extends LDBCQueryDescription
{
    @Override
    public String queryName()
    {
        return "Holy grail";
    }

    @Override
    public String cypher()
    {
        return "// MOTIVATIONAL QUERY (LDBC 2) the holy grail\n" +
               "MATCH (:Person {id:{1}})-[:KNOWS]-(friend:Person) <-[:COMMENT_HAS_CREATOR]-(comment:Comment)\n" +
               "WHERE comment.creationDate <= {2}\n" +
               "RETURN friend.id, comment.id, comment.creationDate\n" +
               "ORDER BY creationDate DESC\n" +
               "LIMIT 20";
    }

    @Override
    public String[] inputDataHeader()
    {
        return new String[]{ "Person", "creationDate" };
    }

    @Override
    public String inputFile()
    {
        return Config.HOLY_GRAIL_PARAMETERS;
    }

    public static QueryDescription INSTANCE = new Query2Description();
}
