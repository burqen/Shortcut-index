package index.logical;

import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class TKeyTest
{
    @Test
    public void equalsAndHash()
    {
        TKey key = new TKey( 1l, 1l );
        TKey same = new TKey( 1, 1l );
        TKey similarId = new TKey( 1, 2l );
        TKey similarProp = new TKey( 2, 1l );

        assertTrue( key.equals( same ) );
        assertTrue( key.hashCode() == same.hashCode() );

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
        TKey key = new TKey( 2, 2l );
        TKey same = new TKey( 2, 2l );
        TKey sameIdlowerProp = new TKey( 2, 1l );
        TKey sameIdhigherProp = new TKey( 2, 3l );
        TKey higherIdLowerProp = new TKey( 3, 1l );
        TKey lowerIdHigherProp = new TKey( 1, 3l );

        assertTrue( key.compareTo( same ) == 0 );

        assertTrue( key.compareTo( sameIdlowerProp ) > 0 );

        assertTrue( key.compareTo( sameIdhigherProp ) < 0 );

        assertTrue( key.compareTo( higherIdLowerProp ) < 0 );

        assertTrue( key.compareTo( lowerIdHigherProp ) > 0 );
    }
}
