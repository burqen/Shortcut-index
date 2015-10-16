package bench.util;

import bench.Environment;

public class Dataset
{
    public final String dbPath;
    public final String dbName;
    public final Environment environment;

    public Dataset( String dbPath, String dbName, Environment environment )
    {
        this.dbPath = dbPath;
        this.dbName = dbName;
        this.environment = environment;
    }
}
