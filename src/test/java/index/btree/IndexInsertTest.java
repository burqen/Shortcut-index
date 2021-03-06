package index.btree;

import index.IdProvider;
import index.SCIndex;
import index.storage.ByteArrayPagedFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class IndexInsertTest extends TestUtils
{
    private Node node;
    private IndexInsert inserter;
    private IdProvider idProvider;

    private static int pageSize = 256;

    private static PagedFile pagedFile;
    private PageCursor cursor;
    private long rootId;

    private PagedFileFactory factory;

    @Parameterized.Parameters
    public static List<Object[]> pageFileFactory() throws IOException
    {
        int cacheSize = 1000000;
        String filePrefix = SCIndex.filePrefix + "01";
        String fileSuffix = SCIndex.indexFileSuffix;
        return Arrays.asList( new Object[][]{
                {
                        (PagedFileFactory) () -> new ByteArrayPagedFile( pageSize )
                },
                {
                        (PagedFileFactory) () -> mapTempFileWithMuninnPageCache( cacheSize, pageSize, filePrefix,
                                fileSuffix )
                }
        } );
    }

    public IndexInsertTest( PagedFileFactory factory )
    {
        this.factory = factory;
    }

    @Before
    public void setUp() throws IOException
    {
        pagedFile = factory.create();

        node = new Node( pageSize );
        idProvider = new IdPool();
        rootId = idProvider.acquireNewId();
        inserter = new IndexInsert( idProvider, node );
        cursor = pagedFile.io( rootId, PagedFile.PF_EXCLUSIVE_LOCK );
        cursor.next();
        node.initializeLeaf( cursor );
        cursor.close();

        // SetUp cursor for test call
        cursor = pagedFile.io( rootId, PagedFile.PF_EXCLUSIVE_LOCK );
        cursor.next();
    }

    @After
    public void tearDown() throws IOException
    {
        cursor.close();
        pagedFile.close();
    }

    @Test
    public void insertSingle() throws IOException
    {
        long x = 0xADCDCC;
        long y = 0xDDADA;
        long[] key = new long[]{x, y};
        long[] value = new long[]{x, y};
        inserter.insert( cursor, key, value );
        int pos = IndexSearch.search( cursor, node, key );
        assertKey( key, node.keyAt( cursor, pos ) );
        assertValue( value, node.valueAt( cursor, pos ) );
        assertKeyCount( 1, node.keyCount( cursor ) );
    }

    @Test
    public void insertMultipleInOrder() throws IOException
    {
        long lCont = 0xDCDCCD;
        for ( int i = 0; i < 5; i++ )
        {
            long l = i * lCont;
            long[] key = new long[]{ l, l };
            long[] value = new long[]{ l+1, l+1 };
            inserter.insert( cursor, key, value );
            assertKeyCount( i + 1, node.keyCount( cursor ) );
        }

        for ( int i = 0; i < 5; i++ )
        {
            long l = i * lCont;
            long[] key = new long[]{ l, l };
            long[] value = new long[]{ l+1, l+1 };
            int pos = IndexSearch.search( cursor, node, key );
            assertKey( key, node.keyAt( cursor, pos ) );
            assertValue( value, node.valueAt( cursor, pos ) );
        }
    }

    @Test
    public void insertMultipleOutOfOrder() throws IOException
    {
        long seed = 1337l;
        Random rnd = new Random( seed );
        List<long[]> inserted = new ArrayList<>();

        for ( int i = 0; i < node.leafMaxKeyCount(); i++ )
        {
            long[] keyAndValue = new long[]{ rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong() };
            long[] key = new long[]{ keyAndValue[0], keyAndValue[1] };
            long[] value = new long[]{ keyAndValue[2], keyAndValue[3] };
            inserter.insert( cursor, key, value );
            inserted.add( keyAndValue );
        }

        Collections.sort( inserted, new Comparator<long[]>()
        {
            @Override
            public int compare( long[] o1, long[] o2 )
            {
                long[] key1 = new long[]{ o1[0], o1[1] };
                long[] key2 = new long[]{ o2[0], o2[1] };
                return Node.KEY_COMPARATOR.compare( key1, key2 );
            }
        });

        for ( int i = 0; i < node.leafMaxKeyCount(); i++ )
        {
            long[] keyAndValue = inserted.get( i );
            long[] key = new long[]{ keyAndValue[0], keyAndValue[1] };
            long[] value = new long[]{ keyAndValue[2], keyAndValue[3] };
            assertKey( key, node.keyAt( cursor, i ) );
            assertValue( value, node.valueAt( cursor, i ) );
        }
    }

    @Test
    public void splitInLeaf() throws IOException
    {
        long lConst = 0xAAACAAA; // Arbitrary value
        long[] key;
        long[] value;
        int maxKeyCount = node.leafMaxKeyCount();
        List<long[]> insertedKeys = new ArrayList<>();
        List<long[]> insertedValues = new ArrayList<>();

        // Insert keys and values up to limit
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            key = new long[]{ i*lConst, i*lConst };
            value = new long[]{ i*lConst, i*lConst };
            inserter.insert( cursor, key, value );
            insertedKeys.add( key );
            insertedValues.add( value );
            assertKeyCount( i + 1, node.keyCount( cursor ) );
        }

        // We should have no split yet
        int keyCount = node.keyCount( cursor );
        assertKeyCount( maxKeyCount, keyCount );

        // This should cause split
        key = new long[]{1, 1};
        value = new long[]{1, 1};
        SplitResult split = inserter.insert( cursor, key, value );
        insertedKeys.add( 1, key );
        insertedValues.add( 1, value );

        assertNotNull( "Expected split", split );
        assertSibling( split.right, node.rightSibling( cursor ) );
        assertSibling( Node.NO_NODE_FLAG, node.leftSibling( cursor ) );
        cursor.next( split.right );
        assertSibling( Node.NO_NODE_FLAG, node.rightSibling( cursor ) );
        assertSibling( split.left, node.leftSibling( cursor ) );

        // Assert left
        cursor.next( split.left );
        int keyCountLeft = node.keyCount( cursor );
        assertKeyCount( (maxKeyCount + 1) / 2, keyCountLeft );
        for ( int i = 0; i < keyCountLeft; i++ )
        {
            assertKey( insertedKeys.get( i ), node.keyAt( cursor, i ) );
            assertValue( insertedValues.get( i ), node.valueAt( cursor, i ) );
        }

        // Assert right
        cursor.next( split.right );
        int keyCountRight = node.keyCount( cursor );
        assertKeyCount( (maxKeyCount + 2) / 2, keyCountRight );
        for ( int i = 0; i < keyCountRight; i++ )
        {
            assertKey( insertedKeys.get( i + keyCountLeft ), node.keyAt( cursor, i ) );
            assertValue( insertedValues.get( i + keyCountLeft ), node.valueAt( cursor, i ) );
        }
    }

    @Test
    public void insertInOrderMultipleSplits() throws IOException
    {
        long rootId;
        for ( int i = 0; i < node.leafMaxKeyCount() * 10; i++ )
        {
            long[] key = new long[]{ i,i };
            long[] value = new long[]{ i,i };
            SplitResult split = inserter.insert( cursor, key, value );

            if ( split != null )
            {
                // New root
                rootId = idProvider.acquireNewId();

                cursor.next( rootId );

                node.initializeInternal( cursor );
                node.setKeyAt( cursor, split.primKey, 0 );
                node.setKeyCount( cursor, 1 );
                node.setChildAt( cursor, split.left, 0 );
                node.setChildAt( cursor, split.right, 1 );
            }
        }

        // Check consistency on every level
        checkKeyConsistencyFromRoot( cursor, node );
    }

    private void checkKeyConsistencyFromRoot( PageCursor cursor, Node node ) throws IOException
    {
        int level = 0;
        long id;
        while ( node.isInternal( cursor ) )
        {
            id = cursor.getCurrentPageId();
            checkKeyConsistencyOnLevel( cursor, node, level );
            cursor.next( id );
            cursor.next( node.childAt( cursor, 0 ) );
        }

        checkKeyConsistencyOnLevel( cursor, node, level );
    }

    private void checkKeyConsistencyOnLevel( PageCursor cursor, Node node, int level ) throws IOException
    {
        long[] lastKey = null;
        while ( true )
        {
            lastKey = checkKeyConsistencyInNode( cursor, node, lastKey, level );
            long rightSibling = node.rightSibling( cursor );
            if ( rightSibling == Node.NO_NODE_FLAG )
            {
                break;
            }
            cursor.next( rightSibling );
        }
    }

    private long[] checkKeyConsistencyInNode( PageCursor cursor, Node node, long[] lastKey, int level )
    {
        int keyCount = node.keyCount( cursor );
        for ( int i = 0; i < keyCount; i++ )
        {
            long[] key = node.keyAt( cursor, i );
            if ( lastKey != null )
            {
                assertTrue( "Expected keys to be in order, but lastKey was " + Arrays.toString( lastKey ) +
                            " and key was " + Arrays.toString( key ) + " on level " + level,
                        Node.KEY_COMPARATOR.compare( lastKey, key ) <= 0 );
            }
            lastKey = key;
        }
        return lastKey;
    }
}
