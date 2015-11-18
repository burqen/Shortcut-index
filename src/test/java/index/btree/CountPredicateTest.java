package index.btree;

import org.junit.Test;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertFalse;

public class CountPredicateTest
{
    @Test
    public void testNoLimit()
    {
        CountPredicate noLimit = CountPredicate.NO_LIMIT;
        for ( int i = -10; i < 10; i++ )
        {
            assertFalse( noLimit.reachedLimit( i ) );
        }
    }

    @Test
    public void testMax()
    {
        CountPredicate max = CountPredicate.max( 10 );
        for ( int i = -10; i < 10; i++ )
        {
            assertFalse( max.reachedLimit( i ) );
        }
        for ( int i = 10; i < 20; i++ )
        {
            assertTrue( max.reachedLimit( i ) );
        }
    }
}
