package index;

import index.btree.Index;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.Direction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

import static junit.framework.TestCase.assertNotNull;

public class SCIndexProviderTest
{
    private File indexFile;
    private File metaFile;
    PageCache pageCache;

    @Before
    public void setup() throws IOException
    {
        PageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        swapper.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        PageCacheTracer tracer = new DefaultPageCacheTracer();

        pageCache = new MuninnPageCache( swapper, 2, 256, tracer );

        indexFile = File.createTempFile( SCIndex.filePrefix, SCIndex.indexFileSuffix );
        metaFile = File.createTempFile( SCIndex.filePrefix, SCIndex.metaFileSuffix );
    }

    @Test
    public void sameDescription() throws IOException
    {
        SCIndexProvider provider = new SCIndexProvider();

        SCIndexDescription desc = new SCIndexDescription( "a", "b", "c", Direction.OUTGOING, "d", null );

        SCIndex index = new Index( pageCache, indexFile, metaFile, desc, 0);

        provider.put( index );

        assertNotNull( provider.get( desc ) );
    }

    @Test
    public void sameDescriptionContent() throws IOException
    {
        SCIndexProvider provider = new SCIndexProvider();

        SCIndexDescription in = new SCIndexDescription( "a", "b", "c", Direction.OUTGOING, "d", null );
        SCIndex index = new Index( pageCache, indexFile, metaFile, in, 0);

        provider.put( index );

        SCIndexDescription fetch = new SCIndexDescription(  "a", "b", "c", Direction.OUTGOING, "d", null );
        assertNotNull( provider.get( fetch ) );
    }
}
