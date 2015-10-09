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
 * [             HEADER         ]|[      KEYS     ]|[     CHILDREN      ]
 * [TYPE][KEYCOUNT][RIGHTSIBLING]|[[KEY][KEY]...##]|[[CHILD][CHILD]...##]
 *  0     1         5              13
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
 * [             HEADER         ]|[      KEYS     ]|[       VALUES      ]
 * [TYPE][KEYCOUNT][RIGHTSIBLING]|[[KEY][KEY]...##]|[[VALUE][VALUE]...##]
 *  0     1         5              13
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
    public static final int HEADER_LENGTH = 13;

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

    // ROUTINES

    public static void initializeLeaf( PageCursor cursor )
    {
        setTypeLeaf( cursor );
        setKeyCount( cursor, 0 );
        setRightSibling( cursor, NO_NODE_FLAG );
    }

    public static void initializeInternal( PageCursor cursor )
    {
        setTypeInternal( cursor );
        setKeyCount( cursor, 0 );
        setRightSibling( cursor, NO_NODE_FLAG );
    }


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

    public static void setRightSibling( PageCursor cursor, long rightSiblingId)
    {
        cursor.putLong( BYTE_POS_RIGHTSIBLING, rightSiblingId );
    }


    // BODY METHODS

    public static long[] keyAt( PageCursor cursor, int pos )
    {
        long[] key = new long[2];
        cursor.setOffset( keyOffset( pos ) );
        key[0] = cursor.getLong();
        key[1] = cursor.getLong();
        return key;
    }

    public static void setKeyAt( PageCursor cursor, long[] key, int pos )
    {
        cursor.setOffset( keyOffset( pos ) );
        cursor.putLong( key[0] );
        cursor.putLong( key[1] );
    }

    public static byte[] keysFromTo( PageCursor cursor, int fromIncluding, int toExcluding )
    {
        byte[] data = new byte[ (toExcluding - fromIncluding) * SIZE_KEY ];
        cursor.setOffset( keyOffset( fromIncluding ) );
        cursor.getBytes( data );
        return data;
    }

    public static void setKeysAt( PageCursor cursor, byte[] keys, int pos )
    {
        cursor.setOffset( keyOffset( pos ) );
        cursor.putBytes( keys );
    }

    public static long[] valueAt( PageCursor cursor, int pos )
    {
        long[] value = new long[2];
        cursor.setOffset( valueOffset( pos ) );
        value[0] = cursor.getLong();
        value[1] = cursor.getLong();
        return value;
    }

    public static void setValueAt( PageCursor cursor, long[] value, int pos )
    {
        cursor.setOffset( valueOffset( pos ) );
        cursor.putLong( value[0] );
        cursor.putLong( value[1] );
    }

    public static byte[] valuesFromTo( PageCursor cursor, int fromIncluding, int toExcluding )
    {
        byte[] data = new byte[ (toExcluding - fromIncluding) * SIZE_VALUE ];
        cursor.setOffset( valueOffset( fromIncluding ) );
        cursor.getBytes( data );
        return data;
    }

    public static byte[] lastValues( PageCursor cursor, int fromPosInclusive, int keyCount )
    {
        byte[] data = new byte[ (keyCount - fromPosInclusive) * SIZE_VALUE ];
        cursor.setOffset( valueOffset( fromPosInclusive ) );
        cursor.getBytes( data );
        return data;
    }

    public static void setValuesAt( PageCursor cursor, byte[] values, int pos )
    {
        cursor.setOffset( valueOffset( pos ) );
        cursor.putBytes( values );
    }


    // HELPERS

    public static int keyOffset( int pos )
    {
        return HEADER_LENGTH + pos * SIZE_KEY;
    }

    public static int valueOffset( int pos )
    {
        return HEADER_LENGTH + IndexGlobal.MAX_KEY_COUNT_LEAF * SIZE_KEY + pos * SIZE_VALUE;
    }

    public static int childOffset( int pos )
    {
        return HEADER_LENGTH + IndexGlobal.MAX_KEY_COUNT_INTERNAL * SIZE_KEY + pos + SIZE_CHILD;
    }
}
