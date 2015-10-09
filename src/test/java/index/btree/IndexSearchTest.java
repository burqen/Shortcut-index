package index.btree;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.io.pagecache.PageCursor;

import static index.btree.IndexSearch.NO_POS;
import static junit.framework.TestCase.assertEquals;

@RunWith( Parameterized.class )
public class IndexSearchTest
{
    @Before
    public void reset()
    {
        Node.setKeyCount( cursor, 0 );
    }

    @Test
    public void searchNoKeys()
    {
        long[] key = new long[]{ 1, 1 };
        int pos = IndexSearch.search( cursor, key );
        assertPos( NO_POS, pos );
    }

    @Test
    public void searchEqualSingleKey()
    {
        long[] key = new long[]{ 1, 1 };
        Node.setKeyAt( cursor, key, 0 );
        Node.setKeyCount( cursor, 1 );

        int pos = IndexSearch.search( cursor, key );
        assertPos( 0, pos );
    }

    @Test
    public void searchSingleKeyWithKeyCountZero()
    {
        long[] key = new long[]{ 1, 1 };
        Node.setKeyAt( cursor, key, 0 );
        Node.setKeyCount( cursor, 0 );
        int pos = IndexSearch.search( cursor, key );
        assertPos( NO_POS, pos );
    }

    @Test
    public void searchEqualMultipleKeys()
    {
        long[] key = new long[]{ 0, 0 };
        long[] key2 = new long[]{ 1, 1 };
        Node.setKeyAt( cursor, key, 0 );
        Node.setKeyAt( cursor, key2, 1 );
        Node.setKeyAt( cursor, key2, 2 );
        Node.setKeyCount( cursor, 3 );
        int pos = IndexSearch.search( cursor, key2 );
        assertPos( 1, pos );
    }

    @Test
    public void searchNotEqualMultipleKeys()
    {
        long[] key1 = new long []{ 1, 1 };
        long[] key2 = new long []{ 2, 2 };
        long[] searchKey = new long[]{ 2,1 };
        Node.setKeyAt( cursor, key1, 0 );
        Node.setKeyAt( cursor, key2, 1 );
        Node.setKeyCount( cursor, 2 );
        int pos = IndexSearch.search( cursor, searchKey );
        assertPos( 1, pos );
    }

    @Test
    public void searchMultipleKeysAllLower()
    {
        long[] highest = new long []{ 5, 5 };
        long[] key1 = new long []{ 1, 1 };
        long[] key2 = new long []{ 2, 2 };
        Node.setKeyAt( cursor, key1, 0 );
        Node.setKeyAt( cursor, key2, 1 );
        Node.setKeyCount( cursor, 2 );
        int pos = IndexSearch.search( cursor, highest );
        assertPos( NO_POS, pos );
    }

    private void assertPos( int expected, int actual )
    {
        assertEquals( "Expected search to return " + expected + " but was instead " + actual, expected, actual );
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        ByteBuffer buffer = ByteBuffer.allocate( 128 );
        PageCursor leafCursor = new ByteBufferCursor( buffer );
        Node.initializeLeaf( leafCursor );

        buffer = ByteBuffer.allocate( 128 );
        PageCursor internalCursor = new ByteBufferCursor( buffer );
        Node.initializeInternal( internalCursor );
        return Arrays.asList( new Object[][]
                {
                        {
                            leafCursor
                        },
                        {
                            internalCursor
                        }
                } );
    }

    private PageCursor cursor;

    public IndexSearchTest( PageCursor cursor )
    {
        this.cursor = cursor;
    }
}
