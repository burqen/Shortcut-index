package bench.queries;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.ReadOperations;

public class Query1Shortcut extends Query1
{
    @Override
    protected void doTraverseFromStart( PrimitiveLongIterator startNodes, ReadOperations operations,
            Measurement measurement )
    {

    }
}
