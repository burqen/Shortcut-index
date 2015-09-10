package index.logical;

import java.lang.reflect.Array;

public abstract class BTreeNode
{
    protected final int order;
    private BTreeNode leftSibling;
    private BTreeNode rightSibling;
    private InternalBTreeNode parent;
    protected TKey[] keys;
    private int keyCount;

    public BTreeNode( int order )
    {
        this.order = order;
        this.keys = new TKey[order*2];
    }

    public abstract BTreeNodeType getNodeType();

    public abstract void insert( TKey key, TValue value );

    public int search( TKey key )
    {
        int i = 0;
        while ( i < getKeyCount() && getKey( i ).compareTo( key ) < 0 )
        {
            i++;
        }
        return 0;
    }

    /**
     *
     * @param pos
     * @param object
     * @param array
     * @param <T>
     * @return
     */
    public static <T> T replace( int pos, T object, T[] array )
    {
        T replaced = array[pos];
        array[pos] = object;
        return replaced;
    }

    /**
     * Assumes left is full and sorted. Right is empty.
     * Overflow comes last in sorting order compared to values in left.
     * @param left
     * @param right
     * @param overflow
     * @param <T>
     */
    public static <T> void split( T[] left, T[] right, T overflow )
    {
        /*
         * Size is even
         * * Left
         *       Overflowed value ---
         *                           v
         *  -----------------------
         * | 0 | 1 | 2 | 3 | 4 | 5 | 6
         *  -----------------------
         *               ^
         * firstToMove --
         *
         *
         * Right
         *  -----------------------
         * |   |   |   |   |   |   |
         *  -----------------------
         *
         * Size is odd
         * Left
         *           Overflowed value ---
         *                               v
         *  ---------------------------
         * | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7
         *  ---------------------------
         *                   ^
         * firstToMove ------
         *
         * Right
         *  ---------------------------
         * |   |   |   |   |   |   |   |
         *  ---------------------------
         */

        if ( left.length != right.length )
        {
            throw new IllegalArgumentException(
                    "When splitting array left and right array need to have equal length." );
        }

        int size = left.length;


        // + 1 if odd + 0 if even
        int firstToMove = size/2 + (size & 1);

        int i = 0;
        while ( i + firstToMove < size )
        {
            right[i] = left[i + firstToMove];
            left[i + firstToMove] = null;
            i++;
        }

        right[i] = overflow;
    }

    // GETTERS and SETTERS

    public int getOrder()
    {
        return order;
    }

    protected void incrementKeyCount()
    {
        keyCount++;
    }

    protected void decrementKeyCount()
    {
        keyCount--;
    }

    protected void setKeyCount( int keyCount )
    {
        this.keyCount = keyCount;
    }

    public void setKey( int i, TKey key )
    {
        keys[i] = key;
    }

    public TKey getKey( int i )
    {
        return keys[i];
    }

    public int getKeyCount()
    {
        return keyCount;
    }

    public void setLeftSibling( BTreeNode leftSibling )
    {
        this.leftSibling = leftSibling;
    }

    public InternalBTreeNode getParent()
    {
        return parent;
    }

    public void setParent( InternalBTreeNode parent )
    {
        this.parent = parent;
    }

    public BTreeNode getLeftSibling()
    {
        return leftSibling;
    }

    public void setRightSibling( BTreeNode rightSibling )
    {
        this.rightSibling = rightSibling;
    }

    public BTreeNode getRightSibling()
    {
        return rightSibling;
    }

    public enum BTreeNodeType
    { InternalNode, LeafNode }
}
