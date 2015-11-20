package bench.queries.impl.lab;

import bench.laboratory.LabEnvironmentGenerator;
import bench.queries.QueryDescription;
import bench.queries.impl.description.LabQuery2Description;
import index.SCKey;
import index.SCResult;
import index.SCResultVisitor;
import index.SCValue;

import java.util.ArrayList;
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

    @Override
    protected SCResultVisitor getVisitor()
    {
        return new SCResultVisitor()
        {
            List<SCResult> list = new ArrayList<>();

            @Override
            public boolean visit( long firstId, long keyProp, long relId, long secondId )
            {
                return list.add( new SCResult( new SCKey( firstId, keyProp ), new SCValue( relId, secondId ) ) );
            }

            @Override
            public long rowCount()
            {
                return list.size();
            }

            @Override
            public void massageRawResult()
            {
                list.sort( ( o1, o2) -> Long.compare( o1.getKey().getProp(), o2.getKey().getProp() ) );
            }

            @Override
            public void limit()
            {
                if ( list.size() > limit )
                {
                    list = list.subList( 0, limit );
                }
            }
        };
    }
}
