package index.btree;

import java.util.Comparator;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Methods to manipulate single node such as set and get header fields,
 * insert and fetch keys, values and children.
 *
 * DESIGN
 *
 * Using Separate design the internal nodes should look like
 *
 * # = empty space
 *
 * [                    HEADER               ]|[      KEYS     ]|[     CHILDREN      ]
 * [TYPE][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING]|[[KEY][KEY]...##]|[[CHILD][CHILD]...##]
 *  0     1         5             13            21
 *
 * Calc offset for key i (starting from 0)
 * HEADER_LENGTH + i * SIZE_KEY
 *
 * Calc offset for child i
 * HEADER_LENGTH + SIZE_KEY * MAX_KEY_COUNT_INTERNAL + i * SIZE_CHILD
 *
 *
 * Using Separate design the leaf nodes should look like
 *
 *
 * [                   HEADER                ]|[      KEYS     ]|[       VALUES      ]
 * [TYPE][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING]|[[KEY][KEY]...##]|[[VALUE][VALUE]...##]
 *  0     1         5             13            21
 *
 * Calc offset for key i (starting from 0)
 * HEADER_LENGTH + i * SIZE_KEY
 *
 * Calc offset for value i
 * HEADER_LENGTH + SIZE_KEY * MAX_KEY_COUNT_LEAF + i * SIZE_VALUE
 *
 */
public class Node
{
    public static final int BYTE_POS_TYPE = 0;
    public static final int BYTE_POS_KEYCOUNT = 1;
    public static final int BYTE_POS_RIGHTSIBLING = 5;
    public static final int BYTE_POS_LEFTSIBLING = 13;
    public static final int HEADER_LENGTH = 21;

    public static final int SIZE_CHILD = 8;
    public static final int SIZE_KEY = 2 * 8;
    public static final int SIZE_VALUE = 2 * 8;

    public static final byte LEAF_FLAG = 1;
    public static final byte INTERNAL_FLAG = 0;
    public static final long NO_NODE_FLAG = -1l;

    private final int internalMaxKeyCount;
    private final int leafMaxKeyCount;


    public static final Comparator<long[]> KEY_COMPARATOR = ( left, right ) -> {
        if ( left.length != 2 || right.length != 2 )
        {
            throw new IllegalArgumentException( "Keys must have length 2 to be compared" );
        }
        int compareId = Long.compare( left[0], right[0] );
        return compareId != 0 ? compareId : Long.compare( left[1], right[1] );
    };

    public Node( int pageSize )
    {
        internalMaxKeyCount = Math.floorDiv( pageSize - (HEADER_LENGTH + SIZE_CHILD),
                SIZE_KEY + SIZE_CHILD);
        leafMaxKeyCount = Math.floorDiv( pageSize - HEADER_LENGTH,
                SIZE_KEY + SIZE_VALUE );
    }

    // ROUTINES

    public void initializeLeaf( PageCursor cursor )
    {
        setTypeLeaf( cursor );
        setKeyCount( cursor, 0 );
        setRightSibling( cursor, NO_NODE_FLAG );
        setLeftSibling( cursor, NO_NODE_FLAG );
    }

    public void initializeInternal( PageCursor cursor )
    {
        setTypeInternal( cursor );
        setKeyCount( cursor, 0 );
        setRightSibling( cursor, NO_NODE_FLAG );
        setLeftSibling( cursor, NO_NODE_FLAG );
    }


    // HEADER METHODS

    public boolean isLeaf( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == LEAF_FLAG;
    }

    public boolean isInternal( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == INTERNAL_FLAG;
    }

    public int keyCount( PageCursor cursor )
    {
        return cursor.getInt( BYTE_POS_KEYCOUNT );
    }

    public long rightSibling( PageCursor cursor )
    {
        return cursor.getLong( BYTE_POS_RIGHTSIBLING );
    }

    public long leftSibling( PageCursor cursor )
    {
        return cursor.getLong( BYTE_POS_LEFTSIBLING );
    }

    public void setTypeLeaf( PageCursor cursor )
    {
        cursor.putByte( BYTE_POS_TYPE, LEAF_FLAG );
    }

    public void setTypeInternal( PageCursor cursor )
    {
        cursor.putByte( BYTE_POS_TYPE, INTERNAL_FLAG );
    }

    public void setKeyCount( PageCursor cursor, int count )
    {
        cursor.putInt( BYTE_POS_KEYCOUNT, count );
    }

    public void setRightSibling( PageCursor cursor, long rightSiblingId )
    {
        cursor.putLong( BYTE_POS_RIGHTSIBLING, rightSiblingId );
    }

    public void setLeftSibling( PageCursor cursor, long leftSiblingId )
    {
        cursor.putLong( BYTE_POS_LEFTSIBLING, leftSiblingId );
    }

    // BODY METHODS

    public long[] keyAt( PageCursor cursor, int pos )
    {
        long[] key = new long[2];
        cursor.setOffset( keyOffset( pos ) );
        key[0] = cursor.getLong();
        key[1] = cursor.getLong();
        return key;
    }

    public void setKeyAt( PageCursor cursor, long[] key, int pos )
    {
        cursor.setOffset( keyOffset( pos ) );
        cursor.putLong( key[0] );
        cursor.putLong( key[1] );
    }

    public byte[] keysFromTo( PageCursor cursor, int fromIncluding, int toExcluding )
    {
        byte[] data = new byte[(toExcluding - fromIncluding) * SIZE_KEY];
        cursor.setOffset( keyOffset( fromIncluding ) );
        cursor.getBytes( data );
        return data;
    }

    public void setKeysAt( PageCursor cursor, byte[] keys, int pos )
    {
        cursor.setOffset( keyOffset( pos ) );
        cursor.putBytes( keys );
    }

    public long[] valueAt( PageCursor cursor, int pos )
    {
        long[] value = new long[2];
        cursor.setOffset( valueOffset( pos ) );
        value[0] = cursor.getLong();
        value[1] = cursor.getLong();
        return value;
    }

    public void setValueAt( PageCursor cursor, long[] value, int pos )
    {
        cursor.setOffset( valueOffset( pos ) );
        cursor.putLong( value[0] );
        cursor.putLong( value[1] );
    }

    public byte[] valuesFromTo( PageCursor cursor, int fromIncluding, int toExcluding )
    {
        byte[] data = new byte[(toExcluding - fromIncluding) * SIZE_VALUE];
        cursor.setOffset( valueOffset( fromIncluding ) );
        cursor.getBytes( data );
        return data;
    }

    public void setValuesAt( PageCursor cursor, byte[] values, int pos )
    {
        cursor.setOffset( valueOffset( pos ) );
        cursor.putBytes( values );
    }

    public long childAt( PageCursor cursor, int pos )
    {
        return cursor.getLong( childOffset( pos ) );
    }

    public void setChildAt( PageCursor cursor, long child, int pos )
    {
        cursor.putLong( childOffset( pos ), child );
    }

    public byte[] childrenFromTo( PageCursor cursor, int fromIncluding, int toExcluding )
    {
        byte[] data = new byte[(toExcluding - fromIncluding) * SIZE_CHILD];
        cursor.setOffset( childOffset( fromIncluding ) );
        cursor.getBytes( data );
        return data;
    }

    public void setChildrenAt( PageCursor cursor, byte[] children, int pos )
    {
        cursor.setOffset( childOffset( pos ) );
        cursor.putBytes( children );
    }

    public int internalMaxKeyCount()
    {
        return internalMaxKeyCount;
    }

    public int leafMaxKeyCount()
    {
        return leafMaxKeyCount;
    }

    // HELPERS

    public int keyOffset( int pos )
    {
        return HEADER_LENGTH + pos * SIZE_KEY;
    }

    public int valueOffset( int pos )
    {
        return HEADER_LENGTH + leafMaxKeyCount * SIZE_KEY + pos * SIZE_VALUE;
    }

    public int childOffset( int pos )
    {
        return HEADER_LENGTH + internalMaxKeyCount * SIZE_KEY + pos * SIZE_CHILD;
    }
}
