package index;

import index.SCKey;
import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class SCKeyTest
{
    @Test
    public void equalsAndHash()
    {
        SCKey key = new SCKey( 1l, 1l );
        SCKey same = new SCKey( 1, 1l );
        SCKey similarId = new SCKey( 1, 2l );
        SCKey similarProp = new SCKey( 2, 1l );

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
        SCKey key = new SCKey( 2, 2l );
        SCKey same = new SCKey( 2, 2l );
        SCKey sameIdlowerProp = new SCKey( 2, 1l );
        SCKey sameIdhigherProp = new SCKey( 2, 3l );
        SCKey higherIdLowerProp = new SCKey( 3, 1l );
        SCKey lowerIdHigherProp = new SCKey( 1, 3l );

        assertTrue( key.compareTo( same ) == 0 );

        assertTrue( key.compareTo( sameIdlowerProp ) > 0 );

        assertTrue( key.compareTo( sameIdhigherProp ) < 0 );

        assertTrue( key.compareTo( higherIdLowerProp ) < 0 );

        assertTrue( key.compareTo( lowerIdHigherProp ) > 0 );
    }
}
