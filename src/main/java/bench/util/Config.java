package bench.util;

import bench.laboratory.Lab;

public class Config
{
    // LDBC
    public static final String INPUT_DIR_LDBC1 = "data/ldbc_sf001_p006_Neo4jDb_substitution_parameters/";

    public static final String INPUT_DIR_LDBC10 = "data/ldbc_sf010_p006_Neo4jDb_substitution_parameters/";

    public static final String QUERY2_PARAMETERS = "query_2_param.txt";

    public static final String QUERY3_PARAMETERS = QUERY2_PARAMETERS;

    public static final String QUERY4_PARAMETERS = "query_4_param.txt";

    public static final String QUERY5_PARAMETERS = "query_5_param.txt";

    public static final String QUERY6_PARAMETERS = "query_6_param.txt";

    public static final String HOLY_GRAIL_PARAMETERS = "holy_grail_param.txt";

    public static final String LDBC_SF001 = "ldbc_sf001_p006_Neo4jDb";

    public static final String LDBC_SF010 = "ldbc_sf010_p006_Neo4jDb";

    // LABORATORY
    public static final String INPUT_DIR_LAB = "data/lab_substitution_parameters/";

    public static final String LAB_QUERY1_PARAMETERS = "lab_query_1_param.txt";

    public static final Lab LAB_8       = new Lab( "LAB_8",     8, 10000 );
    public static final Lab LAB_40      = new Lab( "LAB_40",   40, 10000 );
    public static final Lab LAB_200     = new Lab( "LAB_200", 200, 10000 );
    public static final Lab LAB_400     = new Lab( "LAB_400", 400, 10000 );
    public static final Lab LAB_800     = new Lab( "LAB_800", 800, 10000 );

    // COMMONS
    public static final String GRAPH_DB_FOLDER = "src/main/resources/";

    // BENCHMARK
    public static final String NO_INPUT = "";

    public static final String[] NO_HEADER = new String[0];

    public static final long[] NO_DATA = new long[0];

    public static final int MAX_NUMBER_OF_QUERY_REPETITIONS = 10000;

    public static final String OUTPUT_PATH = "src/main/resources/out/";
}
