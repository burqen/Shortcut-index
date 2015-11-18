package index.btree;

import index.SCIndex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

@RunWith( Parameterized.class )
public class NodeTest extends TestUtils
{
    private final PageCursorFactory factory;
    private PageCursor cursor;
    private Node node;

    long x = 0xABCFACCC;
    long y = 0xFCDCDFFF;
    private static int pageSize = 512;

    @Parameterized.Parameters
    public static List<Object[]> pageCursorFactory() throws IOException
    {
        byte[] data = new byte[pageSize];
        ByteBuffer buffer = ByteBuffer.wrap( data );

        int cacheSize = 1000000;
        String filePrefix = SCIndex.filePrefix + "01";
        String fileSuffix = SCIndex.indexFileSuffix;

        PagedFile pagedFile = mapTempFileWithMuninnPageCache( cacheSize, pageSize, filePrefix, fileSuffix );

        return Arrays.asList( new Object[][]
                {
                        {
                                (PageCursorFactory) () -> new ByteBufferCursor( buffer )
                        },
                        {
                                (PageCursorFactory) () -> pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK )
                        }
                } );
    }

    public NodeTest( PageCursorFactory factory )
    {
        this.factory = factory;
    }

    @Before
    public void setUp() throws IOException
    {
        cursor = factory.create();
        node = new Node( pageSize );
        cursor.next();
    }

    @After
    public void tearDown()
    {
        cursor.close();
    }

    @Test
    public void setAndGetTypeLeaf()
    {
        node.setTypeLeaf( cursor );
        assertTrue( "Expected node to be leaf", node.isLeaf( cursor ) );
        assertFalse( "Expected node to not be internal", node.isInternal( cursor ) );
    }

    @Test
    public void setAndGetTypeInternal()
    {
        node.setTypeInternal( cursor );
        assertTrue( "Expected node to be internal", node.isInternal( cursor ) );
        assertFalse( "Expected node to not be leaf", node.isLeaf( cursor ) );
    }

    @Test
    public void setAndGetKeyCount()
    {
        int keyCount = 120;
        node.setKeyCount( cursor, keyCount );
        int count = node.keyCount( cursor );
        assertEquals( "Expected key count to be " + keyCount + " but was " + count, keyCount, count );
    }

    @Test
    public void setAndGetSiblings()
    {
        long rightSiblingId = 456092;
        long leftSiblingId = Long.MAX_VALUE - 1;
        node.setRightSibling( cursor, rightSiblingId );
        node.setLeftSibling( cursor, leftSiblingId );
        long rightSibling = node.rightSibling( cursor );
        long leftSibling = node.leftSibling( cursor );
        assertEquals( "Expected right sibling to be " + rightSiblingId + " but was " + rightSibling,
                rightSiblingId, rightSibling );
        assertEquals( "Expected left sibling to be " + leftSiblingId + " but was " + leftSibling,
                leftSiblingId, leftSibling );
    }

    @Test
    public void initializeLeaf()
    {
        node.initializeLeaf( cursor );
        assertTrue( "Expected node to be leaf", node.isLeaf( cursor ) );
        assertEquals( "Expected node to have no keys", 0, node.keyCount( cursor ) );
        assertEquals( "Expected node to have no rightSibling", Node.NO_NODE_FLAG,
                node.rightSibling( cursor ) );
        assertEquals( "Expected node to have no leftSibling", Node.NO_NODE_FLAG,
                node.leftSibling( cursor ) );
    }

    @Test
    public void initializeInternal()
    {
        node.initializeInternal( cursor );
        assertTrue( "Expected node to be leaf", node.isInternal( cursor ) );
        assertEquals( "Expected node to have no keys", 0, node.keyCount( cursor ) );
        assertEquals( "Expected node to have no rightSibling", Node.NO_NODE_FLAG,
                node.rightSibling( cursor ) );
        assertEquals( "Expected node to have no leftSibling", Node.NO_NODE_FLAG,
                node.leftSibling( cursor ) );
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
        node.setKeyAt( cursor, overWrittenKey, 1 );
        node.setKeyAt( cursor, key1, 0 );
        node.setKeyAt( cursor, key2, 1 );

        long[] foundKey1 = node.keyAt( cursor, 0 );
        long[] foundKey2 = node.keyAt( cursor, 1 );

        assertTrue( "Expected keys to be equal but key = " + Arrays.toString( key1 ) +
                    ", foundKey = " + Arrays.toString( foundKey1 ),
                Node.KEY_COMPARATOR.compare( key1, foundKey1 ) == 0 );
        assertTrue( "Expected keys to be equal but key = " + Arrays.toString( key2 ) +
                    ", foundKey = " + Arrays.toString( foundKey1 ),
                Node.KEY_COMPARATOR.compare( key2, foundKey2 ) == 0 );
    }

    @Test
    public void setAndGetValue()
    {
        long[] overWrittenValue = new long[]{ 666, 666 };
        long[] value2 = new long[]{ 1, 1 };
        long[] value1 = new long[]{ x, y };
        node.setValueAt( cursor, overWrittenValue, 1 );
        node.setValueAt( cursor, value1, 0 );
        node.setValueAt( cursor, value2, 1 );

        long[] foundValue1 = node.valueAt( cursor, 0 );
        long[] foundValue2 = node.valueAt( cursor, 1 );

        assertValue( value1, foundValue1 );
        assertValue( value2, foundValue2);
    }

    @Test
    public void setAndGetKeysAndValues()
    {
        // Write keys and values
        for ( long i = 0; i < 5; i++ )
        {
            node.setKeyAt( cursor, new long[]{ i, i }, (int)i );
            node.setValueAt( cursor, new long[]{ i, i }, (int)i );
            assertKey( new long[]{i, i}, node.keyAt( cursor, (int) i ) );
            assertValue( new long[]{ i, i }, node.valueAt( cursor, (int)i ) );
        }
        // Read keys
        byte[] oldKeys = node.keysFromTo( cursor, 0, 5 );
        byte[] oldValues = node.valuesFromTo( cursor, 0, 5 );

        // Overwrite keys and values
        for ( int i = 0; i < 5; i++ )
        {
            node.setKeyAt( cursor, new long[]{ 0, 0 }, i );
            node.setValueAt( cursor, new long[]{ 0, 0}, i );
            assertKey( new long[]{ 0,0 }, node.keyAt( cursor, i ) );
            assertValue( new long[]{ 0,0 }, node.valueAt( cursor, i ) );
        }

        // Reset keys and values
        node.setKeysAt( cursor, oldKeys, 0 );
        node.setValuesAt( cursor, oldValues, 0 );

        // Assert
        for ( long i = 0; i < 5; i++ )
        {
            assertKey( new long[]{ i, i }, node.keyAt( cursor, (int) i ) );
            assertValue( new long[]{i, i}, node.valueAt( cursor, (int) i ) );
        }
    }

    @Test
    public void setAndGetChild()
    {
        long overwrite = -1l;
        long a = 0xDACDAC;
        long b = 0xACDCADA;

        node.setChildAt( cursor, overwrite, 0 );
        node.setChildAt( cursor, b, 1 );
        node.setChildAt( cursor, a, 0 );

        assertChild( a, node.childAt( cursor, 0 ) );
        assertChild( b, node.childAt( cursor, 1 ) );
    }

    @Test
    public void setAndGetChildren()
    {
        long childConst = 0xADCDAD;
        // Write children
        for ( int i = 0; i < 5; i++ )
        {
            long child = i*childConst;
            node.setChildAt( cursor, child, i );
            assertChild( child, node.childAt( cursor, i ) );
        }

        // Read children
        byte[] data = node.childrenFromTo( cursor, 0, 5 );

        // Overwrite
        for ( int i = 0; i < 5; i++ )
        {
            node.setChildAt( cursor, 0l, i );
        }

        // Reset children
        node.setChildrenAt( cursor, data, 0 );

        // Assert
        for( int i = 0; i < 5; i++ )
        {
            assertChild( i*childConst, node.childAt( cursor, i ) );
        }
    }
}
