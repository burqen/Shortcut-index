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

    private final IdProvider idProvider;
    private final Node node;

    public IndexInsert( IdProvider idProvider, Node node )
    {
        this.idProvider = idProvider;
        this.node = node;
    }

    public SplitResult insert( PageCursor cursor, long[] key, long[] value ) throws IOException
    {
        // CONTINUE HERE AND SOLVE CURSOR NOT POINTING TO WHAT IS IS SUPPOSED TO POINT AT!!
        long thisId = cursor.getCurrentPageId();
        if ( node.isLeaf( cursor ) )
        {
            return insertInLeaf( cursor, key, value );
        }
        else
        {
            int keyCount = node.keyCount( cursor );
            thisId = cursor.getCurrentPageId();

            int pos = IndexSearch.search( cursor, node, key );
            if ( pos == IndexSearch.NO_POS )
            {
                pos = keyCount;
            }

            cursor.next( node.childAt( cursor, pos ) );

            SplitResult split = insert( cursor, key, value );

            if ( split != null )
            {
                return insertInInternal( cursor, thisId, keyCount, split.primKey, split.right );
            }
        }
        return null;
    }

    private SplitResult insertInInternal( PageCursor cursor, long nodeId, int keyCount, long[] primKey, long rightChild )
            throws IOException
    {
        cursor.next( nodeId );

        if ( keyCount < node.internalMaxKeyCount() )
        {
            // No overflow
            int pos = IndexSearch.search( cursor, node, primKey );
            if ( pos == IndexSearch.NO_POS )
            {
                pos = keyCount;
            }

            // Insert and move keys
            byte[] tmp = node.keysFromTo( cursor, pos, keyCount );
            node.setKeyAt( cursor, primKey, pos );
            node.setKeysAt( cursor, tmp, pos + 1 );

            // Insert and move children
            tmp = node.childrenFromTo( cursor, pos + 1, keyCount + 1 );
            node.setChildAt( cursor, rightChild, pos + 1 );
            node.setChildrenAt( cursor, tmp, pos + 2 );

            // Increase key count
            node.setKeyCount( cursor, keyCount + 1 );

            return null;
        }
        else
        {
            // Overflow
            return splitInternal( cursor, nodeId, primKey, rightChild, keyCount );

        }
    }

    private SplitResult splitInternal( PageCursor cursor, long splittingNodeId, long[] primKey, long newRightChild, int keyCount )

            throws IOException
    {
        long newRight = idProvider.acquireNewNode();

        // First splitting (left) node. Then move on to right node

        // Need old right sibling to set for new right sibling later
        long oldRight = node.rightSibling( cursor );

        // Update right sibling for splitting node
        node.setRightSibling( cursor, newRight );

        // Find position to insert new key
        int pos = IndexSearch.search( cursor, node, primKey );
        if ( pos == IndexSearch.NO_POS )
        {
            pos = keyCount;
        }

        // Arrays to temporarily store keys and children in sorted order.
        byte[] allKeysIncludingNewPrimKey = readRecordsWithInsertRecordInPosition( cursor, primKey, pos, keyCount+1,
                Node.SIZE_KEY, node.keyOffset( 0 ) );
        byte[] allChildrenIncludingNewRightChild = readRecordsWithInsertRecordInPosition( cursor, primKey, pos+1,
                keyCount+2, Node.SIZE_CHILD, node.childOffset( pos+1 ) );


        int keyCountAfterInsert = keyCount + 1;
        int middle = keyCountAfterInsert / 2; // Floor division

        int arrayOffset;
        if ( pos < middle )
        {
            // Write keys to left
            arrayOffset = pos * Node.SIZE_KEY;
            cursor.setOffset( node.keyOffset( pos ) );
            cursor.putBytes( allKeysIncludingNewPrimKey, arrayOffset, (middle - pos) * Node.SIZE_KEY );

            cursor.setOffset( node.childOffset( pos + 1 ) );
            arrayOffset = (pos + 1) * Node.SIZE_CHILD;
            cursor.putBytes( allChildrenIncludingNewRightChild, arrayOffset, (middle - pos) * Node.SIZE_VALUE );
        }

        node.setKeyCount( cursor, middle );

        // Everything in left node should now be updated
        // Ready to start with right node

        cursor.next( newRight );

        // Initialize
        node.initializeInternal( cursor );

        // Right sibling
        node.setRightSibling( cursor, oldRight );

        // Keys
        arrayOffset = (middle + 1) * Node.SIZE_KEY; // NOTE: (middle + 1) don't include middle
        cursor.setOffset( node.keyOffset( 0 ) );
        cursor.putBytes( allKeysIncludingNewPrimKey, arrayOffset, allKeysIncludingNewPrimKey.length - arrayOffset );

        // Children
        arrayOffset = (middle + 1) * Node.SIZE_VALUE;
        cursor.setOffset( node.valueOffset( 0 ) );
        cursor.putBytes( allChildrenIncludingNewRightChild, arrayOffset,
                allChildrenIncludingNewRightChild.length - arrayOffset );

        // Key count
        // NOTE: Not keyCountAfterInsert because middle key is not kept at this level
        node.setKeyCount( cursor, keyCount - middle );

        // Extract middle key (prim key)
        arrayOffset = middle * Node.SIZE_KEY;
        ByteBuffer buffer = ByteBuffer.wrap( allKeysIncludingNewPrimKey, arrayOffset, Node.SIZE_KEY );
        long[] newPrimKey = new long[2];
        newPrimKey[0] = buffer.getLong();
        newPrimKey[1] = buffer.getLong();

        // Populate split result
        SplitResult split = new SplitResult();
        split.primKey = newPrimKey;
        split.left = splittingNodeId;
        split.right = newRight;

        return split;
    }

    private SplitResult insertInLeaf( PageCursor cursor, long[] key, long[] value ) throws IOException
    {
        int keyCount = node.keyCount( cursor );

        if ( keyCount < node.leafMaxKeyCount() )
        {
            // No overflow, insert key and value
            int pos = IndexSearch.search( cursor, node, key );
            if ( pos == IndexSearch.NO_POS )
            {
                pos = keyCount;
            }

            // Insert and move keys
            byte[] tmp = node.keysFromTo( cursor, pos, keyCount );
            node.setKeyAt( cursor, key, pos );
            node.setKeysAt( cursor, tmp, pos + 1 );

            // Insert and move values
            tmp = node.valuesFromTo( cursor, pos, keyCount );
            node.setValueAt( cursor, value, pos );
            node.setValuesAt( cursor, tmp, pos + 1 );

            // Increase key count
            node.setKeyCount( cursor, keyCount + 1 );

            return null; // No split has occurred
        }
        else
        {
            // Overflow, split leaf
            return splitLeaf( cursor, key, value, keyCount );
        }
    }

    /**
     * Cursor is expected to be pointing to full leaf.
     * @param cursor        cursor pointing into full (left) leaf that should be split in two.
     * @param newKey        key to be inserted
     * @param newValue      value to be inserted (in association with key)
     * @return              {@link SplitResult} with necessary information to inform parent
     * @throws IOException  if cursor.next( newRight ) fails
     */
    private SplitResult splitLeaf( PageCursor cursor, long[] newKey, long[] newValue, int keyCount ) throws IOException
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
        long newRight = idProvider.acquireNewNode();

        long oldRight = node.rightSibling( cursor );
        node.setRightSibling( cursor, newRight );

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
        int pos = IndexSearch.search( cursor, node, newKey );
        if ( pos == IndexSearch.NO_POS )
        {
            pos = keyCount;
        }

        // arrays to temporarily store all keys and values
        byte[] allKeysIncludingNewKey = readRecordsWithInsertRecordInPosition( cursor, newKey, pos,
                node.leafMaxKeyCount() + 1, Node.SIZE_KEY, node.keyOffset( 0 ) );
        byte[] allValuesIncludingNewValue = readRecordsWithInsertRecordInPosition( cursor, newValue, pos,
                node.leafMaxKeyCount() + 1, Node.SIZE_VALUE, node.valueOffset( 0 ) );

        int keyCountAfterInsert = keyCount + 1;
        int middle = keyCountAfterInsert / 2; // Floor division

        // allKeysIncludingNewKey should now contain all keys in sorted order and
        // allValuesIncludingNewValue should now contain all values in same order as corresponding keys
        // and are ready to be split between left and newRight.

        // If pos < middle. Write shifted values to left node. Else, don't write anything.
        if ( pos < middle )
        {
            int arrayOffset = pos * Node.SIZE_KEY;
            cursor.setOffset( node.keyOffset( pos ) );
            cursor.putBytes( allKeysIncludingNewKey, arrayOffset, (middle - pos) * Node.SIZE_KEY );

            cursor.setOffset( node.valueOffset( pos ) );
            arrayOffset = pos * Node.SIZE_VALUE;
            cursor.putBytes( allValuesIncludingNewValue, arrayOffset, (middle - pos) * Node.SIZE_VALUE );
        }

        // Key count
        node.setKeyCount( cursor, middle );

        // We now have everything we need to start working on newRight
        // and everything that needs to be updated in left has been so.

        // Initialize
        cursor.next( newRight );
        node.initializeLeaf( cursor );

        // Sibling
        node.setRightSibling( cursor, oldRight );

        // Keys
        int arrayOffset = middle * Node.SIZE_KEY;
        cursor.setOffset( node.keyOffset( 0 ) );
        cursor.putBytes( allKeysIncludingNewKey, arrayOffset, allKeysIncludingNewKey.length - arrayOffset );

        // Values
        arrayOffset = middle * Node.SIZE_VALUE;
        cursor.setOffset( node.valueOffset( 0 ) );
        cursor.putBytes( allValuesIncludingNewValue, arrayOffset, allValuesIncludingNewValue.length - arrayOffset );

        // Key count
        node.setKeyCount( cursor, keyCountAfterInsert - middle );

        SplitResult split = new SplitResult();
        split.left = left;
        split.right = newRight;
        split.primKey = node.keyAt( cursor, 0 );

        // Move cursor back to left
        cursor.next( left );

        return split;
    }

    private byte[] readRecordsWithInsertRecordInPosition( PageCursor cursor, long[] newRecord, int pos,
            int totalNumberOfRecords, int recordSize, int baseRecordOffset )
    {
        byte[] allRecordsIncludingNewRecord = new byte[(totalNumberOfRecords) * recordSize];

        // First read all records

        // Read all records on previous to pos
        cursor.setOffset( baseRecordOffset );
        cursor.getBytes( allRecordsIncludingNewRecord, 0, pos * recordSize );

        // Read newRecord
        ByteBuffer buffer = ByteBuffer.wrap( allRecordsIncludingNewRecord, pos * recordSize, recordSize );
        for ( int i = 0; i < newRecord.length; i++ )
        {
            buffer.putLong( newRecord[i] );
        }

        // Read all records following pos
        cursor.setOffset( baseRecordOffset + pos * recordSize );
        cursor.getBytes( allRecordsIncludingNewRecord, (pos + 1) * recordSize,
                ((totalNumberOfRecords - 1) - pos) * recordSize );
        return allRecordsIncludingNewRecord;
    }
}
