package index.logical;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InternalBTreeNodeTest
{
    int order = 2;
    InternalBTreeNode node;

    @Before
    public void setUpNode()
    {
        node = new InternalBTreeNode( order );
    }

    @Test
    public void setAndGetKeyInsideOfRange()
    {
        node.setKey( 0, new TKey( 1, 2 ) );

        assertTrue( node.getKey( 0 ).equals( new TKey( 1, 2 ) ) );
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void getKeyOutsideOfRange()
    {
        node.getKey( order * 2 );
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void setKeyOutsideOfRange()
    {
        node.setKey( order * 2, null );
    }

    @Test
    public void nodeType()
    {
        assertTrue( node.getNodeType() == BTreeNode.BTreeNodeType.InternalNode );
    }

    @Test
    public void setAndGetChildrenInsideRange()
    {
        BTreeNode child = Mockito.mock( BTreeNode.class );
        node.setChild( 0, child );
        assertEquals( child, node.getChild( 0 ) );
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void setChildrenOutsideRange()
    {
        BTreeNode child = Mockito.mock( BTreeNode.class );
        node.setChild( order*2 + 1, child );
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void getChildrenOutsideRange()
    {
        node.getChild( order*2 + 1 );
    }
}
