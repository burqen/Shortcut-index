package bench.queries.impl.lab;

import bench.queries.QueryDescription;
import bench.queries.impl.description.LabQuery3Description;
import index.SCResult;

import java.util.List;

public class LabQuery3Kernel extends LabQueryKernel
{
    @Override
    protected void massageRawResult( List<SCResult> resultList )
    {
        resultList.sort( ( o1, o2) -> Long.compare( o1.getKey().getProp(), o2.getKey().getProp() ) );
    }

    @Override
    public QueryDescription queryDescription()
    {
        return LabQuery3Description.INSTANCE;
    }

    @Override
    protected boolean filterOnNodeProperty( long prop )
    {
        return false;
    }
}
