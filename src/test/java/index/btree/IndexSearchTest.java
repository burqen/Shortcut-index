package index.btree;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.io.pagecache.PageCursor;

import static junit.framework.TestCase.assertEquals;

@RunWith( Parameterized.class )
public class IndexSearchTest
{
    @Before
    public void reset()
    {
        node.setKeyCount( cursor, 0 );
    }

    @Test
    public void searchNoKeys()
    {
        long[] key = new long[]{ 1, 1 };
        int pos = IndexSearch.search( cursor, node, key );
        assertPos( 0, pos );
    }

    @Test
    public void searchEqualSingleKey()
    {
        long[] key = new long[]{ 1, 1 };
        node.setKeyAt( cursor, key, 0 );
        node.setKeyCount( cursor, 1 );

        int pos = IndexSearch.search( cursor, node, key );
        assertPos( 0, pos );
    }

    @Test
    public void searchSingleKeyWithKeyCountZero()
    {
        long[] key = new long[]{ 1, 1 };
        node.setKeyAt( cursor, key, 0 );
        node.setKeyCount( cursor, 0 );
        int pos = IndexSearch.search( cursor, node, key );
        assertPos( 0, pos );
    }

    @Test
    public void searchEqualMultipleKeys()
    {
        long[] key = new long[]{ 0, 0 };
        long[] key2 = new long[]{ 1, 1 };
        node.setKeyAt( cursor, key, 0 );
        node.setKeyAt( cursor, key2, 1 );
        node.setKeyAt( cursor, key2, 2 );
        node.setKeyCount( cursor, 3 );
        int pos = IndexSearch.search( cursor, node, key2 );
        assertPos( 1, pos );
    }

    @Test
    public void searchNotEqualMultipleKeys()
    {
        long[] key1 = new long []{ 1, 1 };
        long[] key2 = new long []{ 2, 2 };
        long[] searchKey = new long[]{ 2,1 };
        node.setKeyAt( cursor, key1, 0 );
        node.setKeyAt( cursor, key2, 1 );
        node.setKeyCount( cursor, 2 );
        int pos = IndexSearch.search( cursor, node, searchKey );
        assertPos( 1, pos );
    }

    @Test
    public void searchMultipleKeysAllLower()
    {
        long[] highest = new long []{ 5, 5 };
        long[] key1 = new long []{ 1, 1 };
        long[] key2 = new long []{ 2, 2 };
        node.setKeyAt( cursor, key1, 0 );
        node.setKeyAt( cursor, key2, 1 );
        node.setKeyCount( cursor, 2 );
        int pos = IndexSearch.search( cursor, node, highest );
        assertPos( 2, pos );
    }

    private void assertPos( int expected, int actual )
    {
        assertEquals( "Expected search to return " + expected + " but was instead " + actual, expected, actual );
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        int pageSize = 512;
        Node node = new Node( pageSize );
        ByteBuffer buffer = ByteBuffer.allocate( pageSize );
        PageCursor leafCursor = new ByteBufferCursor( buffer );
        node.initializeLeaf( leafCursor );

        buffer = ByteBuffer.allocate( 128 );
        PageCursor internalCursor = new ByteBufferCursor( buffer );
        node.initializeInternal( internalCursor );
        return Arrays.asList( new Object[][]
                {
                        {
                            leafCursor, node
                        },
                        {
                            internalCursor, node
                        }
                } );
    }

    private PageCursor cursor;
    private Node node;

    public IndexSearchTest( PageCursor cursor, Node node )
    {
        this.cursor = cursor;
        this.node = node;
    }
}
