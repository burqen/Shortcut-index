package index.btree;

import index.legacy.TKey;
import index.legacy.TResult;
import index.legacy.TValue;

import java.io.IOException;
import java.util.List;

import org.neo4j.io.pagecache.PageCursor;

public class SeekerRange extends Seeker.CommonSeeker
{
    private final RangePredicate fromPred;
    private final RangePredicate toPred;

    public SeekerRange( Node node, RangePredicate fromPred, RangePredicate toPred )
    {
        super( node );
        this.fromPred = fromPred;
        this.toPred = toPred;
    }

    @Override
    protected void seekLeaf( PageCursor cursor, List<TResult> resultList ) throws IOException
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
            TKey tKey = new TKey( key[0], key[1] );
            long[] value = node.valueAt( cursor, pos );
            TValue tValue = new TValue( value[0], value[1] );
            resultList.add( new TResult( tKey, tValue ) );
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
            seekLeaf( cursor, resultList );
        }
    }

    @Override
    protected void seekInternal( PageCursor cursor, List<TResult> resultList ) throws IOException
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

        seek( cursor, resultList );
    }
}
