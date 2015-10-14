package index.legacy;

import index.SCKey;
import index.SCValue;

import java.io.PrintStream;

public class LegacyLeafBTreeNode extends LegacyBTreeNode
{
    private SCValue[] values;

    public LegacyLeafBTreeNode( int order )
    {
        super( order );
        this.values = new SCValue[order*2];
    }

    public void setValue( int i, SCValue value )
    {
        values[i] = value;
    }

    public SCValue getValue( int i )
    {
        return values[i];
    }

    @Override
    public BTreeNodeType getNodeType()
    {
        return BTreeNodeType.LeafNode;
    }

    @Override
    public void insert( long firstId, long propValue, SCValue value )
    {
        int keyCount = getKeyCount();
        int pos = searchFirstGreaterThanOrEqualTo( firstId, propValue );
        SCKey key = new SCKey( firstId, propValue );
        if ( keyCount < order*2 )
        {
            // No overflow, insert key and value and move other keys and value accordingly.

            // Insert key
            int keyPos = pos;
            while ( keyPos <= keyCount )
            {
                key = replaceKey( keyPos, keys, key.getId(), key.getProp() );
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
            // Overflow, split
            LegacyInternalBTreeNode parent = getParentInMiddleOfSplit();

            LegacyLeafBTreeNode rightLeaf = splitLeafNode( key, value, pos );


            parent.splitInChild( rightLeaf, rightLeaf.getKey( 0 ) );
        }
    }

    @Override
    public int height()
    {
        return 0;
    }

    @Override
    public long totalKeyCount()
    {
        LegacyBTreeNode rightSibling = getRightSibling();
        if ( rightSibling != null )
        {
            return getKeyCount() + rightSibling.totalKeyCount();

        }
        else
        {
            return getKeyCount();
        }
    }

    @Override
    public void printTree( PrintStream out )
    {
        printKeys( out );
        out.print( "\n" );
    }

    /**
     * Split the leaf into two leaves. The newly created right leaf is returned.
     * @param key       {@link index.SCKey} Key to be inserted
     * @param value     {@link index.SCValue} Value to be associated with the key
     * @param pos       Position where the key fit in. Should be in range [0..order*2).
     * @return          {@link LegacyLeafBTreeNode} Newly created right leaf.
     */
    private LegacyLeafBTreeNode splitLeafNode( SCKey key, SCValue value, int pos )
    {
        LegacyLeafBTreeNode rightLeaf = new LegacyLeafBTreeNode( order );

        // Insert key and value in order and keep the last key and value.
        while ( pos < getKeyCount() )
        {
            key = replaceKey( pos, keys, key.getId(), key.getProp() );
            value = replace( pos, value, values );
            pos++;
        }

        // Split
        splitKeys( keys, rightLeaf.keys, key );
        split( values, rightLeaf.values, value );

        // Update key count
        setKeyCount( order );
        rightLeaf.setKeyCount( order + 1 );

        // Set parent
        rightLeaf.setParent( this.getParent() );

        // Update siblings
        rightLeaf.setRightSibling( this.getRightSibling() );
        this.setRightSibling( rightLeaf );

        return rightLeaf;
    }
}
