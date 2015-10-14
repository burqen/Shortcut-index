package index.btree;

import index.SCKey;
import index.SCResult;
import index.SCValue;
import index.Seeker;

import java.io.IOException;
import java.util.List;

import org.neo4j.io.pagecache.PageCursor;

public class RangeSeeker extends Seeker.CommonSeeker
{
    private final RangePredicate fromPred;
    private final RangePredicate toPred;

    public RangeSeeker( RangePredicate fromPred, RangePredicate toPred )
    {
        this.fromPred = fromPred;
        this.toPred = toPred;
    }

    @Override
    protected void seekLeaf( PageCursor cursor, Node node, List<SCResult> resultList ) throws IOException
    {
        int keyCount = node.keyCount( cursor );

        int pos = 0;
        long[] key = node.keyAt( cursor, pos );
        while ( pos < keyCount && fromPred.inRange( key ) < 0 )
        {
            pos++;
            key = node.keyAt( cursor, pos );
        }

        while ( pos < keyCount && toPred.inRange( key ) <= 0 )
        {
            SCKey SCKey = new SCKey( key[0], key[1] );
            long[] value = node.valueAt( cursor, pos );
            SCValue SCValue = new SCValue( value[0], value[1] );
            resultList.add( new SCResult( SCKey, SCValue ) );
            pos++;
            key = node.keyAt( cursor, pos );
        }

        if ( pos < keyCount )
        {
            return;
        }

        // Continue in right sibling
        long rightSibling = node.rightSibling( cursor );
        if ( rightSibling != Node.NO_NODE_FLAG )
        {
            cursor.next( rightSibling );
            seekLeaf( cursor, node, resultList );
        }
    }

    @Override
    protected void seekInternal( PageCursor cursor, Node node, List<SCResult> resultList ) throws IOException
    {
        int keyCount = node.keyCount( cursor );

        int pos = 0;
        long[] key = node.keyAt( cursor, pos );
        while ( pos < keyCount && fromPred.inRange( key ) < 0 )
        {
            pos++;
            key = node.keyAt( cursor, pos );
        }

        cursor.next( node.childAt( cursor, pos ) );

        seek( cursor, node, resultList );
    }
}
