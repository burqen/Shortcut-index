package index.logical;

import java.io.PrintStream;

public abstract class BTreeNode
{
    public static int KEY_SIZE = 2;

    protected final int order;
    private BTreeNode rightSibling;
    private InternalBTreeNode parent;
    protected long[] keys;
    private int keyCount;

    public BTreeNode( int order )
    {
        this.order = order;
        this.keys = new long[order*2*KEY_SIZE];
    }

    public abstract BTreeNodeType getNodeType();

    public abstract void insert( TKey key, TValue value );

    public abstract int height();

    public abstract long totalKeyCount();

    /**
     * This function should be used when getting parent in a split.
     * It makes sure if a split occurs in root a new root will be created.
     * @return The parent after split
     */
    protected InternalBTreeNode getParentInMiddleOfSplit()
    {
        InternalBTreeNode parent = getParent();
        if ( parent == null )
        {
            // This node is missing parent, this is natural if node is currently root
            parent = new InternalBTreeNode( order );
            parent.setChild( 0, this );
            setParent( parent );
        }
        return parent;
    }


    /**
     * Search for the first position where the key on that position is greater than or equal to provided key.
     * @param key   Provided key
     * @return the lowest i for which getKey( i ).compareTo( key ) >= 0 or position just outside of array range if key
     * is greater than every key.
     */
    public int searchFirstGreaterThanOrEqualTo( TKey key )
    {
        int i = 0;
        while ( i < getKeyCount() && getKey( i ).compareTo( key ) < 0 )
        {
            i++;
        }
        return i;
    }

    /**
     *
     * @param pos       Position of element to be replaced
     * @param object    Element to insert on position, pos
     * @param array     Target array
     * @return          Element that was removed from array in favour for object
     */
    public static <T> T replace( int pos, T object, T[] array )
    {
        T replaced = array[pos];
        array[pos] = object;
        return replaced;
    }

    public static TKey replaceKey( int pos, TKey key, long[] keys )
    {
        TKey replaced = new TKey( keys[pos*KEY_SIZE], keys[pos*KEY_SIZE+1] );
        keys[pos*KEY_SIZE] = key.getId();
        keys[pos*KEY_SIZE+1] = key.getProp();
        return replaced;
    }

    /**
     * Assumes left is full and sorted. Right is empty.
     * Overflow comes last in sorting order compared to values in left.
     * This should be used when splitting leaves of children arrays in internal nodes.
     * @param left      Contains the left most elements after split
     * @param right     Contains the right most elements after split
     * @param overflow  Overflowing element is right most element
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

    /**
     * Assumes left is full and sorted. Right is empty.
     * Overflow comes last in sorting order compared to values in left.
     * This should be used when splitting leaves of children arrays in internal nodes.
     * @param left      Contains the left most elements after split
     * @param right     Contains the right most elements after split
     * @param overflow  Overflowing element is right most element
     */
    public static void splitKeys( long[] left, long[] right, TKey overflow )
    {

        if ( left.length != right.length )
        {
            throw new IllegalArgumentException(
                    "When splitting array left and right array need to have equal length." );
        }

        int size = left.length / KEY_SIZE;


        // + KEY_SIZE if odd + 0 if even
        int firstToMove = size/2 + (size & 1);

        int i = 0;
        while ( i + firstToMove < size )
        {
            right[i*KEY_SIZE] = left[(i + firstToMove)*KEY_SIZE];
            right[i*KEY_SIZE + 1] = left[(i + firstToMove)*KEY_SIZE + 1];
            //left[i + firstToMove] = null; // TODO: Should we set to 0?
            i++;
        }

        right[i*KEY_SIZE] = overflow.getId();
        right[i*KEY_SIZE + 1] = overflow.getProp();
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
        keys[i* KEY_SIZE] = key.getId();
        keys[i* KEY_SIZE +1] = key.getProp();
    }

    public TKey getKey( int i )
    {
        return new TKey( keys[i* KEY_SIZE], keys[i* KEY_SIZE +1]);
    }

    public int getKeyCount()
    {
        return keyCount;
    }

    public InternalBTreeNode getParent()
    {
        return parent;
    }

    public void setParent( InternalBTreeNode parent )
    {
        this.parent = parent;
    }

    public void setRightSibling( BTreeNode rightSibling )
    {
        this.rightSibling = rightSibling;
    }

    public BTreeNode getRightSibling()
    {
        return rightSibling;
    }

    protected void printKeys( PrintStream out )
    {
        out.print( "| " );
        for ( int i = 0; i < getKeyCount(); i++ )
        {
            out.print( keys[i] + " " );
        }

        out.print( "| " );

        BTreeNode sibling = getRightSibling();
        if ( sibling != null )
        {
            sibling.printKeys( out );
        }
    }

    public abstract void printTree( PrintStream out );

    public enum BTreeNodeType
    { InternalNode, LeafNode }
}
