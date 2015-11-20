package bench.queries.impl.lab;

import bench.queries.QueryDescription;
import bench.queries.impl.description.LabQuery3Description;
import index.SCKey;
import index.SCResult;
import index.SCResultVisitor;
import index.SCValue;

import java.util.ArrayList;
import java.util.List;

public class LabQuery3Kernel extends LabQueryKernel
{
    @Override
    public QueryDescription queryDescription()
    {
        return LabQuery3Description.INSTANCE;
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
                // Do nothing
            }
        };
    }

    @Override
    protected boolean filterOnNodeProperty( long prop )
    {
        return false;
    }
}
