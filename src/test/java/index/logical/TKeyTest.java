package index.logical;

import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class TKeyTest
{
    @Test
    public void equalsAndHash()
    {
        TKey key = new TKey( 1, new Long( 1 ) );
        TKey same = new TKey( 1, new Long( 1 ) );
        TKey similarId = new TKey( 1, new Long( 2 ) );
        TKey similarProp = new TKey( 2, new Long( 1 ) );

        assertTrue( key.equals( same ) );
        assertTrue( key.hashCode() == same.hashCode() );

        assertFalse( key.equals( null ) );

        // Similar id
        assertFalse( key.equals( similarId ) );
        assertFalse( key.hashCode() == similarId.hashCode() );

        // Similar property
        assertFalse( key.equals( similarProp ) );
        assertFalse( key.hashCode() == similarProp.hashCode() );
    }

    @Test
    public void compareTo()
    {
        TKey key = new TKey( 2, new Long( 2 ) );
        TKey same = new TKey( 2, new Long( 2 ) );
        TKey sameIdlowerProp = new TKey( 2, new Long( 1 ) );
        TKey sameIdhigherProp = new TKey( 2, new Long( 3) );
        TKey higherIdLowerProp = new TKey( 3, new Long( 1 ) );
        TKey lowerIdHigherProp = new TKey( 1, new Long( 3 ) );

        assertTrue( key.compareTo( same ) == 0 );

        assertTrue( key.compareTo( sameIdlowerProp ) > 0 );

        assertTrue( key.compareTo( sameIdhigherProp ) < 0 );

        assertTrue( key.compareTo( higherIdLowerProp ) < 0 );

        assertTrue( key.compareTo( lowerIdHigherProp ) > 0 );
    }
}
