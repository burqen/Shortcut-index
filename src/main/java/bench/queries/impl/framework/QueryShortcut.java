package bench.queries.impl.framework;

import bench.QueryType;
import bench.queries.Query;
import index.SCIndexProvider;

public abstract class QueryShortcut extends Query
{
    protected SCIndexProvider indexes;

    @Override
    public void setIndexProvider( SCIndexProvider indexes )
    {
        this.indexes = indexes;
    }

    @Override
    public QueryType type()
    {
        return QueryType.SHORTCUT;
    }
}
