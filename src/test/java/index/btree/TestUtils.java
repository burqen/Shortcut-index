package index.btree;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class TestUtils
{
    protected void assertKey( long[] expected, long[] actual )
    {
        assertTrue( "Expected key to be " + Arrays.toString( expected ) + " but was " + Arrays.toString( actual )
                , Arrays.equals( expected, actual ) );
    }

    protected void assertValue( long[] expected, long[] actual )
    {
        assertTrue( "Expected values to be " + Arrays.toString( expected ) + " but was " + Arrays.toString( actual ),
                Arrays.equals( expected, actual ) );
    }

    protected void assertChild( long expected, long actual )
    {
        assertEquals( "Expected child to be " + expected + " but was actually " + actual, expected, actual );
    }

    protected void assertKeyCount( int expected, int actual )
    {
        assertEquals( "Expected key count to be " + expected + " but was " + actual, expected, actual );
    }


    protected void assertSibling( long expected, long actual )
    {
        assertEquals( "Expected right sibling to be " + expected + " but was " + actual, expected, actual );
    }

    protected long[] key( long id, long prop )
    {
        return new long[]{ id, prop };
    }


    protected static PagedFile mapTempFileWithMuninnPageCache( int cacheSize, int pageSize, String filePrefix,
            String fileSuffix ) throws IOException
    {
        PageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        swapper.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );

        PageCacheTracer tracer = new DefaultPageCacheTracer();

        int maxPages = cacheSize / pageSize;

        File file = File.createTempFile( filePrefix, fileSuffix );

        MuninnPageCache pageCache = new MuninnPageCache( swapper, maxPages, pageSize, tracer );

        return pageCache.map( file, pageSize );
    }

    protected interface PageCursorFactory
    {
        PageCursor create() throws IOException;
    }

    protected interface PagedFileFactory
    {
        PagedFile create() throws IOException;
    }
}
