package bench.util;

import bench.QueryType;
import bench.Measurement;
import bench.queries.QueryDescription;

public class ResultRow
{
    private QueryDescription query;
    private Measurement kernel;
    private Measurement shortcut;

    public ResultRow( QueryDescription query )
    {
        this.query = query;
    }

    public void addMeasurement( Measurement measurement, QueryType type )
    {
        switch ( type )
        {
        case KERNEL:
            kernel = measurement;
            break;
        case SHORTCUT:
            shortcut = measurement;
            break;
        }
    }

    public QueryDescription query()
    {
        return query;
    }

    public boolean hasMeasurement( QueryType type )
    {
        switch ( type )
        {
        case KERNEL:
            return kernel != null;
        case SHORTCUT:
            return shortcut != null;
        default:
            return false;
        }
    }

    public Measurement measurement( QueryType type )
    {
        switch( type )
        {
        case KERNEL:
            return kernel;
        case SHORTCUT:
            return shortcut;
        default:
            return null;
        }
    }
}
