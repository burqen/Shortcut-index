package index.btree;

import java.util.Comparator;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Used to manipulate fields in nodes (internal and leaf).
 * And to some small extent reason about values. Such as value type.
 */
public class Node
{
    public static final int BYTE_POS_TYPE = 0;
    public static final int BYTE_POS_KEYCOUNT = 1;
    public static final int BYTE_POS_PARENT = 5;
    public static final int BYTE_POS_RIGHTSIBLING = 13;
    public static final int HEADER_LENGTH = 21;

    public static final int SIZE_CHILD  = 8;
    public static final int SIZE_KEY    = 2 * 8;
    public static final int SIZE_VALUE  = 2 * 8;

    public static final byte LEAF_FLAG = 1;
    public static final byte INTERNAL_FLAG = 0;
    public static final long NO_NODE_FLAG = -1l;

    public static final Comparator<long[]> KEY_COMPARATOR = new Comparator<long[]>()
    {
        @Override
        public int compare( long[] left, long[] right )
        {
            if ( left.length != 2 || right.length != 2 )
            {
                throw new IllegalArgumentException( "Keys must have length 2 to be compared" );
            }
            int compareId = Long.compare( left[0], right[0] );
            return compareId != 0 ? compareId : Long.compare( left[1], right[1] );
        }
    };


    // HEADER METHODS

    public static boolean isLeaf( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == LEAF_FLAG;
    }

    public static boolean isInternal( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == INTERNAL_FLAG;
    }

    public static int keyCount( PageCursor cursor )
    {
        return cursor.getInt( BYTE_POS_KEYCOUNT );
    }

    public static long parent( PageCursor cursor )
    {
        return cursor.getLong( BYTE_POS_PARENT );
    }

    public static long rightSibling( PageCursor cursor )
    {
        return cursor.getLong( BYTE_POS_RIGHTSIBLING );
    }

    public static void setTypeLeaf( PageCursor cursor )
    {
        cursor.putByte( BYTE_POS_TYPE, LEAF_FLAG );
    }

    public static void setTypeInternal( PageCursor cursor )
    {
        cursor.putByte( BYTE_POS_TYPE, INTERNAL_FLAG );
    }

    public static void setKeyCount( PageCursor cursor, int count )
    {
        cursor.putInt( BYTE_POS_KEYCOUNT, count );
    }

    public static void setParent( PageCursor cursor, long parentId )
    {
        cursor.putLong( BYTE_POS_PARENT, parentId );
    }

    public static void setRightSibling( PageCursor cursor, long rightSiblingId)
    {
        cursor.putLong( BYTE_POS_RIGHTSIBLING, rightSiblingId );
    }

    public static void initializeLeaf( PageCursor cursor )
    {
        setTypeLeaf( cursor );
        setKeyCount( cursor, 0 );
        setParent( cursor, NO_NODE_FLAG );
        setRightSibling( cursor, NO_NODE_FLAG );
    }

    public static void initializeInternal( PageCursor cursor )
    {
        setTypeInternal( cursor );
        setKeyCount( cursor, 0 );
        setParent( cursor, NO_NODE_FLAG );
        setRightSibling( cursor, NO_NODE_FLAG );
    }

    // BODY METHODS

    public static long[] keyAt( PageCursor cursor, int pos )
    {
        long[] key = new long[2];
        key[0] = cursor.getLong( HEADER_LENGTH + pos * SIZE_KEY );
        key[1] = cursor.getLong( HEADER_LENGTH + pos * SIZE_KEY + 8 ); // 8 is size of half key
        return key;
    }

    public static void setKeyAt( PageCursor cursor, long[] key, int pos )
    {
        cursor.putLong( HEADER_LENGTH + pos * SIZE_KEY, key[0] );
        cursor.putLong( HEADER_LENGTH + pos * SIZE_KEY + 8, key[1] );
    }
}
