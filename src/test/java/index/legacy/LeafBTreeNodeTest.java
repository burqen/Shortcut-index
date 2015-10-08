package index.legacy;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LeafBTreeNodeTest
{
    int order = 2;
    LeafBTreeNode leaf;

    @Before
    public void setUpNode()
    {
        leaf = new LeafBTreeNode( order );
    }

    @Test
    public void setAndGetValueInsideOfRange()
    {
        leaf.setValue( 0, new TValue( 1, 2 ) );

        assertTrue( leaf.getValue( 0 ).equals( new TValue( 1, 2 ) ) );
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void getValueOutsideOfRange()
    {
        leaf.getValue( order*2 );
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void setValueOutsideOfRange()
    {
        leaf.setValue( order*2, null );
    }

    @Test
    public void nodeType()
    {
        assertTrue( leaf.getNodeType() == BTreeNode.BTreeNodeType.LeafNode );
    }

    @Test
    public void keyCountUpdateOnInsertAndSplit()
    {
        leaf.setParent( Mockito.mock( InternalBTreeNode.class ) );

        // Fill leaf up
        for ( int i = 0; i < order*2; i++ )
        {
            leaf.insert( i, i, new TValue( i, i ) );
            assertEquals( i + 1, leaf.getKeyCount() );
        }

        leaf.insert( order*2, order*2, new TValue( order*2, order*2 ) );
        assertEquals( order, leaf.getKeyCount() );
        assertNotNull( "Expected to have a right sibling after split", leaf.getRightSibling() );
    }
}
