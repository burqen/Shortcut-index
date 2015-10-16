package bench.queries.impl.description;

import bench.Environment;
import bench.queries.QueryDescription;

public abstract class LDBCQueryDescription extends QueryDescription
{
    @Override
    public Environment environment()
    {
        return Environment.LDBC;
    }
}
