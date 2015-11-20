package index.btree;

import index.SCResultVisitor;
import index.Seeker;

import java.io.IOException;

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
    protected void seekLeaf( PageCursor cursor, Node node, SCResultVisitor visitor ) throws IOException
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
                long[] value = node.valueAt( cursor, pos );
                if ( visitor.visit( key[0], key[1], value[0], value[1] ) )
                {
                    resultCount++;
                }
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
                seekLeaf( cursor, node, visitor );
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
                long[] value = node.valueAt( cursor, pos );
                if ( visitor.visit( key[0], key[1], value[0], value[1] ) )
                {
                    resultCount++;
                }
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
                seekLeaf( cursor, node, visitor );
            }
        }
    }

    @Override
    protected void seekInternal( PageCursor cursor, Node node, SCResultVisitor visitor ) throws IOException
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

            seek( cursor, node, visitor );
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

            seek( cursor, node, visitor );
        }
    }
}
