package bench.queries.impl.description;

import bench.Environment;
import bench.queries.QueryDescription;

public abstract class LABQueryDescription extends QueryDescription
{
    @Override
    public Environment environment()
    {
        return Environment.LAB;
    }
}
