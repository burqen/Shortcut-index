package bench.queries.impl.ldbc;

import bench.Measurement;
import bench.QueryType;
import index.SCIndex;
import index.SCIndexDescription;
import index.SCIndexProvider;
import index.SCResult;
import index.btree.RangePredicate;
import index.btree.RangeSeeker;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

public class HolyGrailShortcut extends AbstractHolyGrail
{
    public SCIndexDescription indexDescription = new SCIndexDescription( "Person", "Comment",
            "COMMENT_HAS_CREATOR", Direction.INCOMING, null, "creationDate" );
    private SCIndexProvider indexes;

    public HolyGrailShortcut( long limit )
    {
        super( limit );
    }

    @Override
    protected void lastHop( ReadOperations operations, Measurement measurement, long[] inputData, long otherNode,
            List<SCResult> queryResult, int propKey, int commentHasCreator, int commentLabel ) throws IOException
    {
        // LAST HOP
        SCIndex index = indexes.get( indexDescription );

        index.seek( new RangeSeeker( RangePredicate.noLimit( otherNode ),
                        RangePredicate.lowerOrEqual( otherNode, limit ) ),
                queryResult );

        Iterator<SCResult> resultIterator = queryResult.iterator();
        while ( resultIterator.hasNext() )
        {
            SCResult result = resultIterator.next();
        }

    }

    @Override
    public SCIndexDescription indexDescription()
    {
        return indexDescription;
    }

    @Override
    public QueryType type()
    {
        return QueryType.SHORTCUT;
    }

    @Override
    public void setIndexProvider( SCIndexProvider indexes )
    {
        this.indexes = indexes;
    }
}
