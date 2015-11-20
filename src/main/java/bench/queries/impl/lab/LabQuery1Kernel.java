package bench.queries.impl.lab;

import bench.laboratory.LabEnvironmentGenerator;
import bench.queries.QueryDescription;
import bench.queries.impl.description.LabQuery1Description;
import index.SCResultVisitor;

public class LabQuery1Kernel extends LabQueryKernel
{
    private final int percentageOfRange;
    private int lowerBoundary;
    private int upperBoundary;

    public LabQuery1Kernel( int percentageOfRange )
    {
        this.percentageOfRange = percentageOfRange;
        if ( percentageOfRange < 1 || percentageOfRange > 100 )
        {
            throw new IllegalArgumentException( "Percentage is outside range 1-100: " + percentageOfRange );
        }
        lowerBoundary = 0;
        upperBoundary = percentageOfRange * LabEnvironmentGenerator.RANGE_MAX / 100;
    }

    @Override
    protected boolean filterOnNodeProperty( long prop )
    {
        return prop < lowerBoundary || prop >= upperBoundary;
    }

    @Override
    public QueryDescription queryDescription()
    {
        return LabQuery1Description.instance( percentageOfRange );
    }

    @Override
    protected SCResultVisitor getVisitor()
    {
        return new SCResultVisitor.CountingResultVisitor();
    }
}
