package bench.util;

public class Config
{
    public static final String QUERY2_PARAMETERS =
            "data/ldbc_sf001_p006_Neo4jDb_substitution_parameters/query_2_param.txt";

    public static final String QUERY3_PARAMETERS = QUERY2_PARAMETERS;

    public static final String QUERY4_PARAMETERS =
            "data/ldbc_sf001_p006_Neo4jDb_substitution_parameters/query_4_param.txt";

    public static final String QUERY5_PARAMETERS =
            "data/ldbc_sf001_p006_Neo4jDb_substitution_parameters/query_5_param.txt";

    public static final String QUERY6_PARAMETERS =
            "data/ldbc_sf001_p006_Neo4jDb_substitution_parameters/query_6_param.txt";

    public static final String LDBC_SF001 = "ldbc_sf001_p006_Neo4jDb";

    public static final String GRAPH_DB_FOLDER = "src/main/resources/";

    public static final String NO_INPUT = "";

    public static final String[] NO_HEADER = new String[0];

    public static final long[] NO_DATA = new long[0];

    public static final int MAX_NUMBER_OF_QUERY_REPETITIONS = 1000;
}
