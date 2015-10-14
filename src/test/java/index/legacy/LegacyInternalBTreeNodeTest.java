package index.legacy;

import index.SCKey;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LegacyInternalBTreeNodeTest
{
    int order = 2;
    LegacyInternalBTreeNode node;

    @Before
    public void setUpNode()
    {
        node = new LegacyInternalBTreeNode( order );
    }

    @Test
    public void setAndGetKeyInsideOfRange()
    {
        node.setKey( 0, 1, 2 );

        assertTrue( node.getKey( 0 ).equals( new SCKey( 1, 2 ) ) );
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void getKeyOutsideOfRange()
    {
        node.getKey( order * 2 );
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void setKeyOutsideOfRange()
    {
        node.setKey( order * 2, 0, 0 );
    }

    @Test
    public void nodeType()
    {
        assertTrue( node.getNodeType() == LegacyBTreeNode.BTreeNodeType.InternalNode );
    }

    @Test
    public void setAndGetChildrenInsideRange()
    {
        LegacyBTreeNode child = Mockito.mock( LegacyBTreeNode.class );
        node.setChild( 0, child );
        assertEquals( child, node.getChild( 0 ) );
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void setChildrenOutsideRange()
    {
        LegacyBTreeNode child = Mockito.mock( LegacyBTreeNode.class );
        node.setChild( order*2 + 1, child );
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void getChildrenOutsideRange()
    {
        node.getChild( order*2 + 1 );
    }
}
