package bench.util;

import bench.laboratory.Lab;

public class Config
{
    // LDBC
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

    // LABORATORY
    public static final String LAB_QUERY1_PARAMETERS =
            "data/lab_substitution_parameters/lab_query_1_param.txt";

    public static final Lab LAB_10      = new Lab( "LAB_10",   10,   1000 );
    public static final Lab LAB_50      = new Lab( "LAB_50",   50,   1000 );
    public static final Lab LAB_200     = new Lab( "LAB_200",  200,  1000 );
    public static final Lab LAB_500     = new Lab( "LAB_500",  500,  1000 );
    public static final Lab LAB_1000    = new Lab( "LAB_1000", 1000, 1000 );

    // COMMONS
    public static final String GRAPH_DB_FOLDER = "src/main/resources/";

    // BENCHMARK
    public static final String NO_INPUT = "";

    public static final String[] NO_HEADER = new String[0];

    public static final long[] NO_DATA = new long[0];

    public static final int MAX_NUMBER_OF_QUERY_REPETITIONS = 10000;
}
