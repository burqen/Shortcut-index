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
    private final CountPredicate countPred;
    private final boolean descending;
    private int resultCount = 0;

    public RangeSeeker( RangePredicate fromPred, RangePredicate toPred )
    {
        this( fromPred, toPred, false );
    }

    public RangeSeeker( RangePredicate fromPred, RangePredicate toPred, boolean descending )
    {
        this( fromPred, toPred, CountPredicate.NO_LIMIT, descending );
    }

    public RangeSeeker( RangePredicate fromPred, RangePredicate toPred, CountPredicate countPred, boolean descending )
    {
        this.fromPred = fromPred;
        this.toPred = toPred;
        this.descending = descending;
        this.countPred = countPred;
    }

    @Override
    protected void seekLeaf( PageCursor cursor, Node node, List<SCResult> resultList ) throws IOException
    {
        int keyCount = node.keyCount( cursor );

        if ( !descending )
        {
            int pos = 0;
            long[] key = node.keyAt( cursor, pos );
            while ( pos < keyCount && fromPred.inRange( key ) < 0 )
            {
                pos++;
                key = node.keyAt( cursor, pos );
            }

            while ( pos < keyCount && toPred.inRange( key ) <= 0 && !countPred.reachedLimit( resultCount ) )
            {
                SCKey SCKey = new SCKey( key[0], key[1] );
                long[] value = node.valueAt( cursor, pos );
                SCValue SCValue = new SCValue( value[0], value[1] );
                resultList.add( new SCResult( SCKey, SCValue ) );
                resultCount++;
                pos++;
                key = node.keyAt( cursor, pos );
            }

            if ( pos < keyCount || countPred.reachedLimit( resultCount ) )
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
        else
        {
            int pos = keyCount - 1;
            long[] key = node.keyAt( cursor, pos );
            while ( pos > -1 && toPred.inRange( key ) > 0 )
            {
                pos--;
                if ( pos == -1 )
                {
                    break;
                }
                key = node.keyAt( cursor, pos );
            }

            while ( pos > -1 && fromPred.inRange( key ) >= 0 && !countPred.reachedLimit( resultCount ) )
            {
                SCKey SCKey = new SCKey( key[0], key[1] );
                long[] value = node.valueAt( cursor, pos );
                SCValue SCValue = new SCValue( value[0], value[1] );
                resultList.add( new SCResult( SCKey, SCValue ) );
                resultCount++;
                pos--;
                if ( pos == -1 )
                {
                    break;
                }
                key = node.keyAt( cursor, pos );
            }

            if ( pos > -1 || countPred.reachedLimit( resultCount ) )
            {
                return;
            }

            // Continue in left sibling
            long leftSibling = node.leftSibling( cursor );
            if ( leftSibling != Node.NO_NODE_FLAG )
            {
                cursor.next( leftSibling );
                seekLeaf( cursor, node, resultList );
            }
        }
    }

    @Override
    protected void seekInternal( PageCursor cursor, Node node, List<SCResult> resultList ) throws IOException
    {
        int keyCount = node.keyCount( cursor );

        if ( !descending )
        {
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
        else
        {
            int pos = keyCount - 1;
            long[] key = node.keyAt( cursor, pos );
            while ( pos > -1 && toPred.inRange( key ) > 0 )
            {
                pos--;
                if ( pos == -1 )
                {
                    break;
                }
                key = node.keyAt( cursor, pos );
            }

            cursor.next( node.childAt( cursor, pos+1 ) );

            seek( cursor, node, resultList );
        }
    }
}
