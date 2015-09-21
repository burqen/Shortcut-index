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

public class QueryXShortcut extends QueryX
{
    private final ShortcutIndexProvider indexes;

    public QueryXShortcut( ShortcutIndexProvider indexes )
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

            ShortcutIndexService index = indexes.get( indexDescription );

            BTSeeker seeker = new RangeSeeker( maria, null, null );

            List<TResult> hits = new ArrayList<>();
            index.seek( seeker, hits );

            for ( TResult result : hits )
            {
                measurement.row();
            }
        }
    }
}
