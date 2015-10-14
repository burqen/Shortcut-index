package index.legacy;

import index.SCValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LegacyLeafBTreeNodeTest
{
    int order = 2;
    LegacyLeafBTreeNode leaf;

    @Before
    public void setUpNode()
    {
        leaf = new LegacyLeafBTreeNode( order );
    }

    @Test
    public void setAndGetValueInsideOfRange()
    {
        leaf.setValue( 0, new SCValue( 1, 2 ) );

        assertTrue( leaf.getValue( 0 ).equals( new SCValue( 1, 2 ) ) );
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
        assertTrue( leaf.getNodeType() == LegacyBTreeNode.BTreeNodeType.LeafNode );
    }

    @Test
    public void keyCountUpdateOnInsertAndSplit()
    {
        leaf.setParent( Mockito.mock( LegacyInternalBTreeNode.class ) );

        // Fill leaf up
        for ( int i = 0; i < order*2; i++ )
        {
            leaf.insert( i, i, new SCValue( i, i ) );
            assertEquals( i + 1, leaf.getKeyCount() );
        }

        leaf.insert( order*2, order*2, new SCValue( order*2, order*2 ) );
        assertEquals( order, leaf.getKeyCount() );
        assertNotNull( "Expected to have a right sibling after split", leaf.getRightSibling() );
    }
}
