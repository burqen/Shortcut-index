package index.btree;

import index.SCIndexDescription;
import index.SCKey;
import index.SCResult;
import index.Seeker;
import index.storage.ByteArrayPagedFile;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        index.seek( seeker, resultList );

        assertTrue( "Expected result set to be of size " + (toPos - fromPos) + " but was " + resultList.size(),
                resultList.size() == toPos - fromPos );


        for ( int i = fromPos; i < toPos; i++ )
        {
            SCKey resultKey = resultList.get( i - fromPos ).getKey();
            assertKey( insertedKeys.get( i ), key( resultKey.getId(), resultKey.getProp() ) );
        }
    }

    @Parameterized.Parameters
    public static List<Object[]> rangePredicates()
    {
        return Arrays.asList( new Object[][]{
                {
                        greaterOrEqual( 1l, 0l ), lower( 1, 5 ), 11, 16
                },
                {
                        greaterOrEqual( 1l, 0l ), lowerOrEqual( 1, 5 ), 11, 17
                },
                {
                        greater( 1l, 0l ), lower( 1, 5 ), 12, 16
                },
                {
                        greater( 1l, 0l ), lowerOrEqual( 1, 5 ), 12, 17
                },
                {
                        noLimit( 1l ), lowerOrEqual( 1, 5 ), 5, 17
                },
                {
                        noLimit( 1l ), noLimit( 1l ), 5, 18
                },
                {
                        noLimit( 0l ), noLimit( 1l ), 0, 18
                },
                {
                        equalTo( 1, 1 ), equalTo( 1, 1 ), 12, 13
                },
                {
                        acceptAll(), acceptAll(), 0, 23
                }
        } );
    }


    public IndexSeekRangeTest( RangePredicate from, RangePredicate to, int fromPos, int toPos ) throws IOException
    {
        int pageSize = 256;
        ByteArrayPagedFile pagedFile = new ByteArrayPagedFile( pageSize );
        index = new Index( pagedFile, Mockito.mock( SCIndexDescription.class ) );
        this.fromPos = fromPos;
        this.toPos = toPos;
        this.seeker = new RangeSeeker( from, to );
    }
}
