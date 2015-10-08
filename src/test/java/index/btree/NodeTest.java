package index.btree;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.io.pagecache.PageCursor;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class NodeTest
{
    private PageCursor cursor;

    long x = 0xABCFACCC;
    long y = 0xFCDCDFFF;
    private int keyCount = 120;
    private long parentId = 3076029;
    private long rightSiblingId = 456092;

    @Before
    public void setup()
    {
        ByteBuffer buffer = ByteBuffer.allocate( 128 );
        cursor = new ByteBufferCursor( buffer );
    }

    @Test
    public void setAndGetTypeLeaf()
    {
        Node.setTypeLeaf( cursor );
        assertTrue( "Expected node to be leaf", Node.isLeaf( cursor ) );
        assertFalse( "Expected node to not be internal", Node.isInternal( cursor ) );
    }

    @Test
    public void setAndGetTypeInternal()
    {
        Node.setTypeInternal( cursor );
        assertTrue( "Expected node to be internal", Node.isInternal( cursor ) );
        assertFalse( "Expected node to not be leaf", Node.isLeaf( cursor ) );
    }

    @Test
    public void setAndGetKeyCount()
    {
        Node.setKeyCount( cursor, keyCount );
        int count = Node.keyCount( cursor );
        assertEquals( "Expected key count to be " + keyCount + " but was " + count, keyCount, count );
    }

    @Test
    public void setAndGetParent()
    {
        Node.setParent( cursor, parentId );
        long id = Node.parent( cursor );
        assertEquals( "Expected parent to be " + parentId + " but was " + id, parentId, id );
    }

    @Test
    public void setAndGetRightSibling()
    {
        Node.setRightSibling( cursor, rightSiblingId );
        long sibling = Node.rightSibling( cursor );
        assertEquals( "Expected right sibling to be " + rightSiblingId + " but was " + sibling,
                rightSiblingId, sibling );
    }

    @Test
    public void initializeLeaf()
    {
        Node.initializeLeaf( cursor );
        assertTrue( "Expected node to be leaf", Node.isLeaf( cursor ) );
        assertEquals( "Expected node to have no keys", 0, Node.keyCount( cursor ) );
        assertEquals( "Expected node to have no parent", Node.NO_NODE_FLAG, Node.parent( cursor ) );
        assertEquals( "Expected node to have no rightSibling", Node.NO_NODE_FLAG,
                Node.rightSibling( cursor ) );
    }

    @Test
    public void initializeInternal()
    {
        Node.initializeInternal( cursor );
        assertTrue( "Expected node to be leaf", Node.isInternal( cursor ) );
        assertEquals( "Expected node to have no keys", 0, Node.keyCount( cursor ) );
        assertEquals( "Expected node to have no parent", Node.NO_NODE_FLAG, Node.parent( cursor ) );
        assertEquals( "Expected node to have no rightSibling", Node.NO_NODE_FLAG,
                Node.rightSibling( cursor ) );
    }


    @Test
    public void keyComparator()
    {
        Comparator<long[]> comp = Node.KEY_COMPARATOR;

        long[] key = new long[]{ x, y };
        long[] same = new long[]{ x, y };
        long[] higherId = new long[]{ x+1, y };
        long[] lowerId = new long[]{ x-1, y };
        long[] higherProp = new long[]{ x, y+1 };
        long[] lowerProp = new long[]{ x, y-1 };

        assertTrue( "Expected key and same to be equal", comp.compare( key, same ) == 0 );
        assertTrue( "Expected key to be less than higherId", comp.compare( key, higherId ) < 0 );
        assertTrue( "Expected key to be less than higherProp", comp.compare( key, higherProp ) < 0 );
        assertTrue( "Expected key to be greater than lowerId", comp.compare( key, lowerId ) > 0 );
        assertTrue( "Expected key to be greater than lowerProp", comp.compare( key, lowerProp ) > 0 );
    }

    @Test
    public void setAndGetKey()
    {
        long[] overWrittenKey = new long[]{ 666, 666 };
        long[] key2 = new long[]{ 1, 1 };
        long[] key1 = new long[]{ x, y };
        Node.setKeyAt( cursor, overWrittenKey, 1 );
        Node.setKeyAt( cursor, key1, 0 );
        Node.setKeyAt( cursor, key2, 1 );

        long[] foundKey1 = Node.keyAt( cursor, 0 );
        long[] foundKey2 = Node.keyAt( cursor, 1 );

        assertTrue( "Expected keys to be equal but key = " + Arrays.toString( key1 ) +
                    ", foundKey = " + Arrays.toString( foundKey1 ),
                Node.KEY_COMPARATOR.compare( key1, foundKey1 ) == 0 );
        assertTrue( "Expected keys to be equal but key = " + Arrays.toString( key2 ) +
                    ", foundKey = " + Arrays.toString( foundKey1 ),
                Node.KEY_COMPARATOR.compare( key2, foundKey2 ) == 0 );
    }
}
