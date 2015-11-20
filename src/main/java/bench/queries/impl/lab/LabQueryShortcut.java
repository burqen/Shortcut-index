package bench.queries.impl.lab;

import bench.Measurement;
import bench.queries.impl.framework.QueryShortcut;
import index.SCIndex;
import index.SCIndexDescription;
import index.SCResultVisitor;
import index.Seeker;

import java.io.IOException;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public abstract class LabQueryShortcut extends QueryShortcut
{
    public SCIndexDescription indexDescription = new SCIndexDescription( "Person", "Comment",
            "CREATED", Direction.OUTGOING, null, "date" );

    @Override
    protected long doRunQuery( ReadOperations operations, Measurement measurement, long[] inputData )
            throws IOException
    {
        SCResultVisitor visitor = getVisitor();
        try
        {
            int firstLabel = operations.labelGetForName( indexDescription.firstLabel );

            final long start = inputData[0];

            if ( !operations.nodeHasLabel( start, firstLabel ) )
            {
                throw new IllegalArgumentException(
                        "Node[" + start + "] did not have label " + indexDescription.firstLabel + " as expected. " +
                        "Use correct input file." );
            }

            SCIndex index = indexes.get( indexDescription );


            index.seek( seeker( start ), visitor );
        }
        catch ( EntityNotFoundException e )
        {
            e.printStackTrace();
        }

        visitor.massageRawResult();
        visitor.limit();
        return visitor.rowCount();
    }

    protected abstract Seeker seeker( long start );

    @Override
    public SCIndexDescription indexDescription()
    {
        return indexDescription;
    }
}
