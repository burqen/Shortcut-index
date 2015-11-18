package bench.queries.impl.lab;

import bench.laboratory.LabEnvironmentGenerator;
import bench.queries.QueryDescription;
import bench.queries.impl.description.LabQuery2Description;
import index.SCResult;

import java.util.List;

public class LabQuery2Kernel extends LabQueryKernel
{
    private final int limit;
    private int lowerBoundary;
    private int upperBoundary;

    public LabQuery2Kernel( int limit )
    {
        this.limit = limit;
        if ( limit < 1 )
        {
            throw new IllegalArgumentException( "Limit is less than 1: " + limit );
        }
        lowerBoundary = 0;
        upperBoundary = LabEnvironmentGenerator.RANGE_MAX;
    }

    @Override
    protected void massageRawResult( List<SCResult> resultList )
    {
        resultList.sort( ( o1, o2) -> Long.compare( o1.getKey().getProp(), o2.getKey().getProp() ) );
    }

    @Override
    protected List<SCResult> limit( List<SCResult> resultList )
    {
        if ( resultList.size() > limit )
        {
            return resultList.subList( 0, limit );
        }
        else
        {
            return resultList;
        }
    }

    @Override
    protected boolean filterOnNodeProperty( long prop )
    {
        return prop < lowerBoundary || prop >= upperBoundary;
    }

    @Override
    public QueryDescription queryDescription()
    {
        return LabQuery2Description.instance( limit );
    }
}
