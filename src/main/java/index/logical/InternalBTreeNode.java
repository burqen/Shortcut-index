package index.logical;

import java.io.PrintStream;

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
        children[searchFirstGreaterThanOrEqualTo( key )].insert( key, value );
    }

    @Override
    public int height()
    {
        return 1 + getChild( 0 ).height();
    }

    @Override
    public long totalKeyCount()
    {
        return getChild( 0 ).totalKeyCount();
    }

    @Override
    public void printTree( PrintStream out )
    {
        printKeys( out );
        out.print( "\n" );
        children[0].printTree( out );
    }

    /**
     * On split, child will call this method to inform it's parent that a split has occurred and tell what key to be
     * inserted and what the new right child is.
     * @param rightChild    Right child after split in child, to be added as child in this internal node.
     * @param key           Key sent from child after split.
     */
    public void splitInChild( BTreeNode rightChild, TKey key )
    {
        int pos = searchFirstGreaterThanOrEqualTo( key );
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

            incrementKeyCount();
        }
        else
        {
            // Overflow
            splitInternalNode( key, rightChild, pos );
        }
    }

    private void splitInternalNode( TKey newKey, BTreeNode newChild, int pos )
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
            for ( int i = pos; i < order; i++ )
            {
                newKey = replace( i, newKey, keys );

            }
            middleKey = newKey;
        }
        else // (pos > order
        {
            for ( int i = pos-1; i >= order; i-- )
            {
                newKey = replace( i, newKey, keys );
            }

            // middle is at pos [order + 1]
            middleKey = newKey;
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

        // Update key count
        setKeyCount( order );
        rightInternalNode.setKeyCount( order );

        // Split children array
        split( children, rightInternalNode.children, newChild );

        // Update parent in children
        for ( int i = 0; i <= rightInternalNode.getKeyCount(); i++ )
        {
            rightInternalNode.children[i].setParent( rightInternalNode );
        }

        InternalBTreeNode parent = getParentInMiddleOfSplit();

        // Set parent
        rightInternalNode.setParent( parent );

        // Update siblings
        rightInternalNode.setRightSibling( this.getRightSibling() );
        this.setRightSibling( rightInternalNode );


        parent.splitInChild( rightInternalNode, middleKey );
    }
}
