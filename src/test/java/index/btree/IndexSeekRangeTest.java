package index.btree;

import index.SCIndex;
import index.SCIndexDescription;
import index.SCKey;
import index.SCResult;
import index.SCResultVisitor;
import index.Seeker;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

import static index.btree.RangePredicate.acceptAll;
import static index.btree.RangePredicate.equalTo;
import static index.btree.RangePredicate.greater;
import static index.btree.RangePredicate.greaterOrEqual;
import static index.btree.RangePredicate.lower;
import static index.btree.RangePredicate.lowerOrEqual;
import static index.btree.RangePredicate.noLimit;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class IndexSeekRangeTest extends TestUtils
{
    private final int fromPos;
    private final int toPos;
    private final boolean descending;
    private Index index;
    private Seeker seeker;

    private List<long[]> insertedKeys = new ArrayList<>();

    @Before
    public void setupKeys() throws IOException
    {
        insertedKeys.add( key( 0, MIN_VALUE ) ); // 0
        insertedKeys.add( key( 0, -1        ) );
        insertedKeys.add( key( 0, 0         ) );
        insertedKeys.add( key( 0, 1         ) );
        insertedKeys.add( key( 0, MAX_VALUE ) );
        insertedKeys.add( key( 1, MIN_VALUE ) ); // 5
        insertedKeys.add( key( 1, -5        ) );
        insertedKeys.add( key( 1, -4        ) );
        insertedKeys.add( key( 1, -3        ) );
        insertedKeys.add( key( 1, -2        ) );
        insertedKeys.add( key( 1, -1        ) ); // 10
        insertedKeys.add( key( 1, 0         ) );
        insertedKeys.add( key( 1, 1         ) );
        insertedKeys.add( key( 1, 2         ) );
        insertedKeys.add( key( 1, 3         ) );
        insertedKeys.add( key( 1, 4         ) ); // 15
        insertedKeys.add( key( 1, 5         ) );
        insertedKeys.add( key( 1, MAX_VALUE ) );
        insertedKeys.add( key( 2, MIN_VALUE ) );
        insertedKeys.add( key( 2, -1        ) );
        insertedKeys.add( key( 2, 0         ) ); // 20
        insertedKeys.add( key( 2, 1         ) );
        insertedKeys.add( key( 2, MAX_VALUE ) ); // 22

        for ( int i = 0; i < insertedKeys.size(); i++ )
        {
            index.insert( insertedKeys.get( i ), new long[]{ i,i } );
        }
    }

    @Test
    public void resultShouldMatchExpectation() throws IOException
    {
        List<SCResult> resultList = new ArrayList<>();
        index.seek( seeker, SCResultVisitor.storeInListVisitor( resultList ) );

        assertTrue( "Expected result set to be of size " + (toPos - fromPos) + " but was " + resultList.size(),
                resultList.size() == toPos - fromPos );


        if ( descending )
        {
            for ( int i = 0; i < resultList.size(); i++ )
            {
                SCKey resultKey = resultList.get( i ).getKey();
                assertKey( insertedKeys.get( toPos - 1 - i ), key( resultKey.getId(), resultKey.getProp() ) );
            }
        }
        else
        {
            for ( int i = 0; i < resultList.size(); i++ )
            {
                SCKey resultKey = resultList.get( i ).getKey();
                assertKey( insertedKeys.get( i + fromPos ), key( resultKey.getId(), resultKey.getProp() ) );
            }
        }
    }

    @Parameterized.Parameters
    public static List<Object[]> rangePredicates() throws IOException
    {
        PageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        swapper.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        PageCacheTracer tracer = new DefaultPageCacheTracer();
        int cachePageSize = 256;
        int maxPages = 1000000 / cachePageSize; // 1 MB

        PageCache pageCache = new MuninnPageCache( swapper, maxPages, cachePageSize, tracer );

        return Arrays.asList( new Object[][]{
                {
                        greaterOrEqual( 1l, 0l ), lower( 1, 5 ), 11, 16, pageCache, false, 0
                },
                {
                        greaterOrEqual( 1l, 0l ), lowerOrEqual( 1, 5 ), 11, 17, pageCache, false, 0
                },
                {
                        greater( 1l, 0l ), lower( 1, 5 ), 12, 16, pageCache, false, 0
                },
                {
                        greater( 1l, 0l ), lowerOrEqual( 1, 5 ), 12, 17, pageCache, false, 0
                },
                {
                        noLimit( 1l ), lowerOrEqual( 1, 5 ), 5, 17, pageCache, false, 0
                },
                {
                        noLimit( 1l ), noLimit( 1l ), 5, 18, pageCache, false, 0
                },
                {
                        noLimit( 0l ), noLimit( 1l ), 0, 18, pageCache, false, 0
                },
                {
                        equalTo( 1, 1 ), equalTo( 1, 1 ), 12, 13, pageCache, false, 0
                },
                {
                        acceptAll(), acceptAll(), 0, 23, pageCache, false, 0
                },
                // DESCENDING
                {
                        greaterOrEqual( 1l, 0l ), lower( 1, 5 ), 11, 16, pageCache, true, 0
                },
                {
                        greaterOrEqual( 1l, 0l ), lowerOrEqual( 1, 5 ), 11, 17, pageCache, true, 0
                },
                {
                        greater( 1l, 0l ), lower( 1, 5 ), 12, 16, pageCache, true, 0
                },
                {
                        greater( 1l, 0l ), lowerOrEqual( 1, 5 ), 12, 17, pageCache, true, 0
                },
                {
                        noLimit( 1l ), lowerOrEqual( 1, 5 ), 5, 17, pageCache, true, 0
                },
                {
                        noLimit( 1l ), noLimit( 1l ), 5, 18, pageCache, true, 0
                },
                {
                        noLimit( 0l ), noLimit( 1l ), 0, 18, pageCache, true, 0
                },
                {
                        equalTo( 1, 1 ), equalTo( 1, 1 ), 12, 13, pageCache, true, 0
                },
                {
                        acceptAll(), acceptAll(), 0, 23, pageCache, true, 0
                },
                // ASCENDING + count limit
                {
                        greaterOrEqual( 1l, 0l ), lower( 1, 5 ), 11, 13, pageCache, false, 2
                },
                {
                        greaterOrEqual( 1l, 0l ), lowerOrEqual( 1, 5 ), 11, 14, pageCache, false, 3
                },
                {
                        greater( 1l, 0l ), lower( 1, 5 ), 12, 15, pageCache, false, 3
                },
                {
                        greater( 1l, 0l ), lowerOrEqual( 1, 5 ), 12, 16, pageCache, false, 4
                },
                {
                        noLimit( 1l ), lowerOrEqual( 1, 5 ), 5, 14, pageCache, false, 9
                },
                {
                        noLimit( 1l ), noLimit( 1l ), 5, 10, pageCache, false, 5
                },
                {
                        noLimit( 0l ), noLimit( 1l ), 0, 15, pageCache, false, 15
                },
                {
                        equalTo( 1, 1 ), equalTo( 1, 1 ), 12, 13, pageCache, false, 1
                },
                {
                        acceptAll(), acceptAll(), 0, 5, pageCache, false, 5
                },
                // DESCENDING + countLimit
                {
                        greaterOrEqual( 1l, 0l ), lower( 1, 5 ), 13, 16, pageCache, true, 3
                },
                {
                        greaterOrEqual( 1l, 0l ), lowerOrEqual( 1, 5 ), 15, 17, pageCache, true, 2
                },
                {
                        greater( 1l, 0l ), lower( 1, 5 ), 13, 16, pageCache, true, 3
                },
                {
                        greater( 1l, 0l ), lowerOrEqual( 1, 5 ), 14, 17, pageCache, true, 3
                },
                {
                        noLimit( 1l ), lowerOrEqual( 1, 5 ), 7, 17, pageCache, true, 10
                },
                {
                        noLimit( 1l ), noLimit( 1l ), 16, 18, pageCache, true, 2
                },
                {
                        noLimit( 0l ), noLimit( 1l ), 9, 18, pageCache, true, 9
                },
                {
                        equalTo( 1, 1 ), equalTo( 1, 1 ), 12, 13, pageCache, true, 1
                },
                {
                        acceptAll(), acceptAll(), 3, 23, pageCache, true, 20
                },
        } );
    }

    public IndexSeekRangeTest( RangePredicate from, RangePredicate to, int fromPos, int toPos, PageCache pageCache,
            boolean descending, int maxResultCount )
            throws IOException
    {
        this.descending = descending;
        File indexFile = File.createTempFile( SCIndex.filePrefix, SCIndex.indexFileSuffix );
        File metaFile = File.createTempFile( SCIndex.filePrefix, SCIndex.metaFileSuffix );
        SCIndexDescription description = new SCIndexDescription();
        index = new Index( pageCache, indexFile, metaFile, description, 256 );
        this.fromPos = fromPos;
        this.toPos = toPos;
        CountPredicate countPredicate;
        if ( maxResultCount < 1 )
        {
            countPredicate = CountPredicate.NO_LIMIT;
        }
        else
        {
            countPredicate = CountPredicate.max( maxResultCount );
        }
        seeker = new RangeSeeker( from, to, countPredicate, descending );
    }
}
