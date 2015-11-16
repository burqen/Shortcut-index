package bench.queries.impl.framework;

import bench.QueryType;
import bench.queries.Query;
import index.SCIndexProvider;
import index.SCResult;

public abstract class QueryShortcut extends Query
{
    protected SCIndexProvider indexes;

    @Override
    public void setIndexProvider( SCIndexProvider indexes )
    {
        this.indexes = indexes;
    }

    protected abstract boolean filterResultRow( SCResult resultRow );

    @Override
    public QueryType type()
    {
        return QueryType.SHORTCUT;
    }
}
