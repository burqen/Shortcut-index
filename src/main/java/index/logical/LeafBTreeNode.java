package index.logical;

public class LeafBTreeNode extends BTreeNode
{
    private TValue[] values;

    public LeafBTreeNode( int order )
    {
        super( order );
        this.values = new TValue[order*2];
    }

    public void setValue( int i, TValue value )
    {
        values[i] = value;
    }

    public TValue getValue( int i )
    {
        return values[i];
    }

    @Override
    public BTreeNodeType getNodeType()
    {
        return BTreeNodeType.LeafNode;
    }

    @Override
    public void insert( TKey key, TValue value )
    {
        int keyCount = getKeyCount();
        int pos = search( key );
        if ( keyCount < order*2 )
        {
            // No overflow, insert key and value and move other keys and value accordingly.

            int keyPos = pos;
            // Insert key
            while ( keyPos <= keyCount )
            {
                key = replace( keyPos, key, keys );
                keyPos++;
            }

            // Insert value
            int valuePos = pos;
            while( valuePos <= keyCount )
            {
                value = replace( valuePos, value, values );
                valuePos++;
            }

            incrementKeyCount();
        }
        else
        {
            // Overflow
            LeafBTreeNode rightLeaf = splitLeafNode( key, value, pos );

            getParent().splitInChild( this, rightLeaf, rightLeaf.getKey( 0 ) );
        }
    }

    /**
     * Split the leaf into two leaves. The newly created right leaf is returned.
     * @param key       {@link index.logical.TKey} Key to be inserted
     * @param value     {@link index.logical.TValue} Value to be associated with the key
     * @param pos       Position where the key fit in. Should be in range [0..order*2).
     * @return          {@link index.logical.LeafBTreeNode} Newly created right leaf.
     */
    private LeafBTreeNode splitLeafNode( TKey key, TValue value, int pos )
    {
        LeafBTreeNode rightLeaf = new LeafBTreeNode( order );

        // Insert key and value in order and keep the last key and value.
        while ( pos < getKeyCount() )
        {
            key = replace( pos, key, keys );
            value = replace( pos, value, values );
            pos++;
        }

        // Split
        split( keys, rightLeaf.keys, key );
        split( values, rightLeaf.values, value );

        // Update key count
        setKeyCount( order );
        rightLeaf.setKeyCount( order + 1 );

        // Set parent
        rightLeaf.setParent( this.getParent() );

        // Update siblings
        rightLeaf.setRightSibling( this.getRightSibling() );
        rightLeaf.setLeftSibling( this );
        this.setRightSibling( rightLeaf );

        return rightLeaf;
    }
}
