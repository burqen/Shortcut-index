package index.logical;

import org.neo4j.register.Register;

public class InternalBTreeNode extends BTreeNode
{
    private BTreeNode[] children;

    public InternalBTreeNode( int order )
    {
        super( order );
        children = new BTreeNode[order*2 + 1];
    }

    public void setChild( int i, BTreeNode child )
    {
        children[i] = child;
    }

    public BTreeNode getChild( int i )
    {
        return children[i];
    }

    @Override
    public BTreeNodeType getNodeType()
    {
        return BTreeNodeType.InternalNode;
    }

    @Override
    public void insert( TKey key, TValue value )
    {
        children[search( key )].insert( key, value );
    }

    /**
     * Child calls this method when a split occurs.
     * @param leftChild
     * @param rightChild
     * @param key
     */
    public void splitInChild( BTreeNode leftChild, BTreeNode rightChild, TKey key )
    {
        int pos = search( key );
        int keyCount = getKeyCount();
        if ( keyCount < order*2 )
        {
            // No overflow
            while ( pos <= keyCount )
            {
                key = replace( pos, key, keys );
                rightChild = replace( pos+1, rightChild, children );
                pos++;
            }
        }
        else
        {
            // Overflow
            InternalBTreeNode rightInternalNode = splitInternalNode( key, rightChild, pos );

            getParent().splitInChild( this, rightInternalNode, rightInternalNode.getKey( 0 ) );
        }
    }

    private InternalBTreeNode splitInternalNode( TKey newKey, BTreeNode newChild, int pos )
    {
        InternalBTreeNode rightInternalNode = new InternalBTreeNode( order );


        // Identify middle key, extract it and insert new key in correct order
        TKey middleKey;
        if ( pos == order )
        {
            // key is middle
            middleKey = newKey;
        }
        else if ( pos < order )
        {
            // middle is at pos [order - 1]
            middleKey = keys[order - 1];
            for ( int i = pos; i < order; i++ )
            {
                newKey = replace( i, newKey, keys );
            }
        }
        else // (pos > order
        {
            // middle is at pos [order + 1]
            middleKey = keys[order + 1];
            for ( int i = pos; i > order ; i-- )
            {
                newKey = replace( i, newKey, keys );
            }
        }

        // Move right most keys from this (left node after split) to right (right node after split)
        for ( int i = order; i < order*2; i++ )
        {
            rightInternalNode.keys[i - order] = this.keys[i];
            this.keys[i] = null;
        }

        // Insert new child in correct order
        while ( pos < getKeyCount() )
        {
            // New child should be put in on pos + 1 because it should be placed to the right of the new key
            newChild = replace( pos + 1, newChild, children );
            pos++;
        }

        split( children, rightInternalNode.children, newChild );

        // Update key count
        setKeyCount( order );
        rightInternalNode.setKeyCount( order );

        // Set parent
        rightInternalNode.setParent( this.getParent() );

        // Update siblings
        rightInternalNode.setRightSibling( this.getRightSibling() );
        rightInternalNode.setLeftSibling( this );
        this.setRightSibling( rightInternalNode );

        getParent().splitInChild( this, rightInternalNode, middleKey );

        return rightInternalNode;
    }
}
