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

    private InternalBTreeNode splitInternalNode( TKey key, BTreeNode rightChild, int pos )
    {
        InternalBTreeNode rightInternalNode = new InternalBTreeNode( order );

        // Insert key and children in order and keep the last key and child.
        while ( pos < getKeyCount() )
        {
            key = replace( pos, key, keys );
            rightChild = replace( pos + 1, rightChild, children );
            pos++;
        }

        // TODO: DONT USE THIS SPLIT HERE, IT'S WRONG! WE NEED 2 DIFFERENT SPLIT. ONE THAT KEEPS MIDDLE VALUE AND ONE THAT DON'T
        // Split
        split( keys, rightInternalNode.keys, key );
        split( children, rightInternalNode.children, rightChild );

        // Update key count
        setKeyCount( order );
        rightInternalNode.setKeyCount( order );

        // Set parent
        rightInternalNode.setParent( this.getParent() );

        // Update siblings
        rightInternalNode.setRightSibling( this.getRightSibling() );
        rightInternalNode.setLeftSibling( this );
        this.setRightSibling( rightInternalNode );

        return rightInternalNode;
    }
}
