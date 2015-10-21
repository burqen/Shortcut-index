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
import java.util.Collection;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static index.btree.TestUtils.PageCursorFactory;
import static index.btree.TestUtils.mapTempFileWithMuninnPageCache;
import static junit.framework.TestCase.assertEquals;

@RunWith( Parameterized.class )
public class IndexSearchTest
{
    private final PageCursorFactory factory;

    private PageCursor cursor;
    private Node node;

    @Before
    public void reset() throws IOException
    {
        cursor = factory.create();
        cursor.next();

        node.setKeyCount( cursor, 0 );
    }

    @After
    public void tearDown()
    {
        cursor.close();
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
    public static Collection<Object[]> data() throws IOException
    {
        int pageSize = 512;
        Node node = new Node( pageSize );

        byte[] leaf = new byte[pageSize];
        byte[] internal = new byte[pageSize];

        ByteBufferCursor leafCursor = new ByteBufferCursor( ByteBuffer.wrap( leaf ) );
        leafCursor.next();
        node.initializeLeaf( leafCursor );
        leafCursor.close();

        ByteBufferCursor internalCursor = new ByteBufferCursor( ByteBuffer.wrap( internal ) );
        internalCursor.next();
        node.initializeLeaf( internalCursor );
        internalCursor.close();

        String leafFilePrefix = SCIndex.filePrefix + "leaf";
        String internalFilePrecix = SCIndex.filePrefix + "internal";
        String fileSuffix = SCIndex.indexFileSuffix;

        int cacheSize = 1000000;
        PagedFile leafFile = mapTempFileWithMuninnPageCache( cacheSize, pageSize, leafFilePrefix, fileSuffix );
        PageCursor munninLeaf = leafFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK );
        munninLeaf.next();
        node.initializeLeaf( munninLeaf );
        munninLeaf.close();

        PagedFile internalFile = mapTempFileWithMuninnPageCache( cacheSize, pageSize, internalFilePrecix, fileSuffix );
        PageCursor munninInternal = internalFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK );
        munninInternal.next();
        node.initializeInternal( munninInternal );
        munninInternal.close();

        return Arrays.asList( new Object[][]
                {
                        {
                                (PageCursorFactory) () -> new ByteBufferCursor( ByteBuffer.wrap( leaf ) ), node
                        },
                        {
                                (PageCursorFactory) () -> new ByteBufferCursor( ByteBuffer.wrap( internal ) ), node
                        },
                        {
                                (PageCursorFactory) () -> leafFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK ), node
                        },
                        {
                                (PageCursorFactory) () -> internalFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK ), node
                        }
                } );
    }

    public IndexSearchTest( PageCursorFactory factory, Node node )
    {
        this.factory = factory;
        this.node = node;
    }
}
