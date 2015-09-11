package index.logical;

import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class TValueTest
{
    @Test
    public void equalsAndHash()
    {
        TValue key = new TValue( 1, new Long( 1 ) );
        TValue same = new TValue( 1, new Long( 1 ) );
        TValue similarId = new TValue( 1, new Long( 2 ) );
        TValue similarProp = new TValue( 2, new Long( 1 ) );

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
}
