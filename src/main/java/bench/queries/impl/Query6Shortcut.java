package bench.queries.impl;

import bench.queries.framework.Measurement;
import bench.queries.framework.QueryDescription;
import bench.queries.framework.QueryShortcut;
import bench.queries.impl.description.Query6Description;
import index.logical.ShortcutIndexProvider;
import index.logical.TResult;

import java.util.List;

import org.neo4j.kernel.api.ReadOperations;

public class Query6Shortcut extends QueryShortcut
{
    public Query6Shortcut( ShortcutIndexProvider indexes )
    {
        super( indexes );
    }

    @Override
    protected List<TResult> doRunQuery( ReadOperations operations, Measurement measurement, long[] inputData )
    {
        return null;
    }

    @Override
    protected boolean filterResultRow( TResult resultRow )
    {
        return false;
    }

    @Override
    public QueryDescription queryDescription()
    {
        return Query6Description.instance;
    }
}
