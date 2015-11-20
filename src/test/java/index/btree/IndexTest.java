package index.btree;

import index.SCIndex;
import index.SCIndexDescription;
import index.SCKey;
import index.SCResult;
import index.SCResultVisitor;
import index.SCValue;
import index.btree.util.SeekerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.neo4j.graphdb.Direction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

import static org.junit.Assert.assertEquals;

public class IndexTest extends TestUtils
{
    Index index;
    int maxPages = 64;
    int cachePageSize = 512;
    PageSwapperFactory swapper = new SingleFilePageSwapperFactory();
    PageCacheTracer tracer = new DefaultPageCacheTracer();
    PageCache pageCache;
    File indexFile;
    File metaFile;
    SCIndexDescription description = new SCIndexDescription( "A", "B", "R", Direction.OUTGOING, null, "prop" );
    List<SCResult> data;
    List<SCResult> secondBatchData;
    int nbrOfInserts = 10000;


    @Before
    public void setUp() throws IOException
    {
        swapper.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        pageCache = new MuninnPageCache( swapper, maxPages, cachePageSize, tracer );
        indexFile = File.createTempFile( SCIndex.filePrefix, SCIndex.indexFileSuffix );
        metaFile = File.createTempFile( SCIndex.filePrefix, SCIndex.metaFileSuffix );

        data = new ArrayList<>();
        secondBatchData = new ArrayList<>();

        Random rnd = new Random( 1337 );
        for ( int i = 0; i < nbrOfInserts; i++ )
        {
            data.add( new SCResult(
                            new SCKey( rnd.nextLong(), rnd.nextLong() ),
                            new SCValue( rnd.nextLong(), rnd.nextLong() ) )
            );
            secondBatchData.add( new SCResult(
                            new SCKey( rnd.nextLong(), rnd.nextLong() ),
                            new SCValue( rnd.nextLong(), rnd.nextLong() ) )
            );
        }
    }

    @After
    public void tearDown() throws IOException
    {
        pageCache.close();
    }

    @Test
    public void createAndLoadNewIndexWithRandomValues() throws IOException
    {
        index = new Index( pageCache, indexFile, metaFile, description, cachePageSize );
        writeData( index, data );
        index.close();

        index = new Index( pageCache, indexFile, metaFile );
        assertData( index, data );
        index.close();
    }

    @Test
    public void writeToLoadedIndex() throws IOException
    {
        index = new Index( pageCache, indexFile, metaFile, description, cachePageSize );
        writeData( index, data );
        index.close();

        index = new Index( pageCache, indexFile, metaFile );
        assertData( index, data );
        writeData( index, secondBatchData );
        assertData( index, data );
        assertData( index, secondBatchData );
        index.close();

        index = new Index( pageCache, indexFile, metaFile );
        assertData( index, secondBatchData );
        assertData( index, data );
        index.close();
    }

    private void writeData( SCIndex index, List<SCResult> data ) throws IOException
    {
        for ( SCResult toInsert: data )
        {
            SCKey key = toInsert.getKey();
            SCValue value = toInsert.getValue();
            index.insert( new long[]{ key.getId(), key.getProp() }, new long[]{ value.getRelId(), value.getNodeId() } );
        }
    }

    private void assertData( SCIndex index, List<SCResult> data ) throws IOException
    {
        List<SCResult> resultList = new ArrayList<>();
        for ( SCResult inserted : data )
        {
            long id = inserted.getKey().getId();
            long prop = inserted.getKey().getProp();

            index.seek( SeekerFactory.exactMatch( id, prop ), SCResultVisitor.storeInListVisitor( resultList ) );
        }

        assertResultList( data, resultList );
    }

    private void assertResultList( List<SCResult> data, List<SCResult> resultList )
    {
        assertEquals( "Expected lists to have equal size", data.size(), resultList.size() );
        for ( int i = 0; i < data.size(); i++ )
        {
            assertEquals(
                    String.format( "Result differed in index %d. Inserted data was %s but result from seek was %s",
                            i, data.get( i ), resultList.get( i ) ), data.get( i ), resultList.get( i ) );
        }
    }
}
