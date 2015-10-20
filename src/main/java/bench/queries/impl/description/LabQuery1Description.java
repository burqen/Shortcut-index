package bench.queries.impl.description;

import bench.queries.QueryDescription;
import bench.util.Config;

public class LABQuery1Description extends LABQueryDescription
{
    private final int percentageOfRange;

    public LABQuery1Description( int percentageOfRange )
    {
        this.percentageOfRange = percentageOfRange;
    }
    @Override
    public String queryName()
    {
        return String.format( "Lab Query1 %03d%%", percentageOfRange );
    }

    @Override
    public String cypher()
    {
        return "// " + queryName() + "\n" +
               "// Range covers % of total range -> % of neighbourhood because of uniform distribution\n" +
               "MATCH (p:Person)-[r:CREATED]->(c:Comment)\n" +
               "WHERE id(p) = {1} AND c.date < {2}\n" +
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

    public static QueryDescription instance( int percentageOfRange )
    {
        return new LABQuery1Description( percentageOfRange );
    }
}
