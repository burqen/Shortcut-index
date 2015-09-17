package bench.queries;

import index.logical.BTSeeker;
import index.logical.RangeSeeker;
import index.logical.ShortcutIndexProvider;
import index.logical.ShortcutIndexService;
import index.logical.TResult;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipDataExtractor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

import static org.neo4j.graphdb.Direction.INCOMING;

public class Query1Shortcut extends Query1
{
    private final ShortcutIndexProvider indexes;

    public Query1Shortcut( ShortcutIndexProvider indexes )
    {
        this.indexes = indexes;
    }

    @Override
    protected void doTraverseFromStart( PrimitiveLongIterator startNodes, ReadOperations operations,
            Measurement measurement )
    {
        while ( startNodes.hasNext() )
        {
            final long maria = startNodes.next();

            ShortcutIndexService index = indexes.get( null );

            BTSeeker seeker = new RangeSeeker( maria, null, null );

            List<TResult> hits = new ArrayList<>();
            index.seek( seeker, hits );

            for ( TResult result : hits )
            {
                measurement.countSuccesses();
            }
        }
    }
}
