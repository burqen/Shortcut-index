package index.btree;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Implementation of the insert algorithm in this B+ tree including split.
 * Takes storage format into consideration.
 */
public class IndexInsert
{

    private final Index index;

    public IndexInsert( Index index )
    {
        this.index = index;
    }

    public SplitResult insert( PageCursor cursor, long[] key, long[] value ) throws IOException
    {
        SplitResult split;

        if ( Node.isLeaf( cursor ) )
        {
            split = insertInLeaf( cursor, key, value );
        }
        else
        {
            split = insertInInternal( cursor, key, value );
        }

        return split;
    }

    private SplitResult insertInInternal( PageCursor cursor, long[] key, long[] value )
    {
        throw new NotImplementedException();
    }

    private SplitResult insertInLeaf( PageCursor cursor, long[] key, long[] value ) throws IOException
    {
        int keyCount = Node.keyCount( cursor );

        if ( keyCount < IndexGlobal.MAX_KEY_COUNT_LEAF )
        {
            // No overflow, insert key and value
            int pos = IndexSearch.search( cursor, key );

            // Insert and move keys
            byte[] tmp = Node.keysFromTo( cursor, pos, keyCount );
            Node.setKeyAt( cursor, key, pos );
            Node.setKeysAt( cursor, tmp, pos + 1 );

            // Insert and move values
            tmp = Node.valuesFromTo( cursor, pos, keyCount );
            Node.setValueAt( cursor, value, pos );
            Node.setValuesAt( cursor, tmp, pos + 1 );

            // Increase key count
            Node.setKeyCount( cursor, keyCount + 1 );

            return null; // No split has occurred
        }
        else
        {
            // Overflow, split leaf
            return splitLeaf( cursor, key, value );
        }
    }

    /**
     * Cursor is expected to be pointing to full leaf. Is left pointing into right leaf after split/
     * @param cursor        cursor pointing into full (left) leaf that should be split in two.
     *                      will point into new right leaf on return
     * @param newKey        key to be inserted
     * @param newValue      value to be inserted (in association with key)
     * @return              {@link SplitResult} with necessary information to inform parent
     * @throws IOException  if cursor.next( newRight ) fails
     */
    private SplitResult splitLeaf( PageCursor cursor, long[] newKey, long[] newValue ) throws IOException
    {
        // To avoid moving cursor between pages we do all operations on left node first.
        // Save data that needs transferring and then add it to right node.

        // UPDATE SIBLINGS
        //
        // Before split
        // newRight is leaf node to be inserted between left and oldRight
        // [left] -> [oldRight]
        //
        //     [newRight]
        //
        // After split
        // [left] -> [newRight] -> [oldRight]
        //

        long left = cursor.getCurrentPageId();
        long newRight = index.acquireNewNode();


        long oldRight = Node.rightSibling( cursor );
        Node.setRightSibling( cursor, newRight );

        // BALANCE KEYS AND VALUES
        // Two different scenarios
        // Before split
        // [key1]<=[key2]<=[key3]<=[key4]<=[key5]   (<= greater than or equal to)
        //                           ^
        //                           |
        //                      pos  |
        // [newKey] -----------------
        //
        // After split
        // Left
        // [key1]<=[key2]<=[key3]
        //
        // Right
        // [newKey][key4][key5]
        //
        // Before split
        // [key1]<=[key2]<=[key3]<=[key4]<=[key5]   (<= greater than or equal to)
        //   ^
        //   | pos
        //   |
        // [newKey]
        //
        // After split
        // Left
        // [newKey]<=[key1]<=[key2]
        //
        // Right
        // [key3][key4][key5]
        //

        // Position where newKey / newValue is to be inserted
        int pos = IndexSearch.search( cursor, newKey );

        // array to temporarily store all keys
        byte[] allKeysIncludingNewKey = new byte[IndexGlobal.MAX_KEY_COUNT_LEAF * Node.SIZE_KEY];
        byte[] allValuesIncludingNewValue = new byte[IndexGlobal.MAX_KEY_COUNT_LEAF * Node.SIZE_VALUE];

        // First read all keys

        // Read all keys lower than newKey
        cursor.setOffset( Node.keyOffset( 0 ) );
        cursor.getBytes( allKeysIncludingNewKey, 0, pos * Node.SIZE_KEY );

        // Read newKey
        ByteBuffer buffer = ByteBuffer.wrap( allKeysIncludingNewKey, pos * Node.SIZE_KEY, Node.SIZE_KEY );
        buffer.putLong( newKey[0] );
        buffer.putLong( newKey[1] );

        // Read all keys greater than or equal to newKey
        cursor.setOffset( Node.keyOffset( pos ) );
        cursor.getBytes( allKeysIncludingNewKey, (pos + 1) * Node.SIZE_KEY,
                (IndexGlobal.MAX_KEY_COUNT_LEAF - pos) * Node.SIZE_KEY );

        // Then read all values in the same fashion
        // Read all "lower values"
        cursor.setOffset( Node.valueOffset( 0 ) );
        cursor.getBytes( allValuesIncludingNewValue, 0, pos * Node.SIZE_VALUE );

        // Read newValue
        buffer = ByteBuffer.wrap( allValuesIncludingNewValue, pos * Node.SIZE_VALUE, Node.SIZE_VALUE );
        buffer.putLong( newValue[0] );
        buffer.putLong( newValue[1] );

        // Read all values "greater values"
        cursor.setOffset( Node.valueOffset( pos ) );
        cursor.getBytes( allValuesIncludingNewValue, (pos + 1) * Node.SIZE_VALUE,
                (IndexGlobal.MAX_KEY_COUNT_LEAF - pos) * Node.SIZE_VALUE );

        // allKeysIncludingNewKey should now contain all keys in sorted order and
        // allValuesIncludingNewValue should now contain all values in same order as corresponding keys
        // and are ready to be split between left and newRight.

        // We do not need to overwrite keys or values moved from left to newRight
        Node.setKeyCount( cursor, IndexGlobal.MAX_KEY_COUNT_LEAF - pos );

        // We now have everything we need to start working on newRight
        // and everything that needs to be updated in left has been so.

        cursor.next( newRight );
        Node.setRightSibling( cursor, oldRight );

        // Keys
        int arrayOffset = pos * Node.SIZE_KEY;
        cursor.setOffset( Node.keyOffset( 0 ) );
        cursor.putBytes( allKeysIncludingNewKey, arrayOffset, allKeysIncludingNewKey.length - arrayOffset );

        // Values
        arrayOffset = pos * Node.SIZE_VALUE;
        cursor.setOffset( Node.valueOffset( 0 ) );
        cursor.putBytes( allValuesIncludingNewValue, arrayOffset, allValuesIncludingNewValue.length - arrayOffset );

        SplitResult split = new SplitResult();
        split.left = left;
        split.right = newRight;
        split.primKey = Node.keyAt( cursor, 0 );

        return split;
    }
}
