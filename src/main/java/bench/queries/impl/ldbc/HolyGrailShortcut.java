package bench.queries.impl.ldbc;

import bench.Measurement;
import bench.QueryType;
import index.SCIndex;
import index.SCIndexDescription;
import index.SCIndexProvider;
import index.SCKey;
import index.SCResult;
import index.SCResultVisitor;
import index.SCValue;
import index.btree.CountPredicate;
import index.btree.RangePredicate;
import index.btree.RangeSeeker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
            SCResultVisitor visitor, int propKey, int commentHasCreator, int commentLabel ) throws IOException
    {
        // LAST HOP
        SCIndex index = indexes.get( indexDescription );

        index.seek( new RangeSeeker( RangePredicate.noLimit( otherNode ),
                        RangePredicate.lowerOrEqual( otherNode, limit ), CountPredicate.max( 20 ), true ),
                visitor );
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
                Collections.sort( list, ( o1, o2 ) -> -Long.compare( o1.getKey().getProp(), o2.getKey().getProp() ) );
            }

            @Override
            public void limit()
            {
                if ( list.size() > 20 )
                {
                    list = list.subList( 0, 20 );
                }
            }
        };
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
