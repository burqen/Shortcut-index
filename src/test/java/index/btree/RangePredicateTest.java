package index.btree;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class RangePredicateTest
{
    private RangePredicate pred;
    private int firstPos;
    private int lastPos;

    private List<long[]> keys = new ArrayList<>();

    @Before
    public void setupKeys()
    {
        keys.add( key( 0, MIN_VALUE ) ); // 0
        keys.add( key( 0, -1        ) );
        keys.add( key( 0, 0         ) );
        keys.add( key( 0, 1         ) );
        keys.add( key( 0, MAX_VALUE ) );
        keys.add( key( 1, MIN_VALUE ) ); // 5
        keys.add( key( 1, -1        ) );
        keys.add( key( 1, 0         ) );
        keys.add( key( 1, 1         ) );
        keys.add( key( 1, MAX_VALUE ) );
        keys.add( key( 2, MIN_VALUE ) ); // 10
        keys.add( key( 2, -1        ) );
        keys.add( key( 2, 0         ) );
        keys.add( key( 2, 1         ) );
        keys.add( key( 2, MAX_VALUE ) );
    }

    private long[] key( long id, long prop )
    {
        return new long[]{ id, prop };
    }

    @Test
    public void predicateGiveCorrectInRange()
    {
        assertTrue( "Wrong input parameters, firstPos needs to be < lastPos", firstPos < lastPos );
        for ( int i = 0; i < keys.size(); i++ )
        {
            long[] key = keys.get( i );
            if ( i < firstPos )
            {
                assertTrue( pred.inRange( key ) < 0 );
            }
            else if ( i < lastPos )
            {
                assertTrue( pred.inRange( key ) == 0 );
            }
            else
            {
                assertTrue( pred.inRange( key ) > 0 );
            }
        }
    }

    @Parameterized.Parameters
    public static List<Object[]> loggers() {
        return Arrays.asList( new Object[][]{
                {
                        RangePredicate.noLimit( 1 ), 5, 10
                },
                {
                        RangePredicate.lower( 1, 1 ), 5, 8
                },
                {
                        RangePredicate.lowerOrEqual( 1, 1 ), 5, 9
                },
                {
                        RangePredicate.greater( 1, -1 ), 7, 10
                },
                {
                        RangePredicate.greaterOrEqual( 1, -1 ), 6, 10
                }
        } );
    }

    public RangePredicateTest( RangePredicate pred, int firstPos, int lastPos )
    {
        this.pred = pred;
        this.firstPos = firstPos;
        this.lastPos = lastPos;
    }
}
