package index.logical;

import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class TValueTest
{
    @Test
    public void equalsAndHash()
    {
        TValue key = new TValue( 1, 1l );
        TValue same = new TValue( 1, 1l );
        TValue similarId = new TValue( 1, 2l );
        TValue similarProp = new TValue( 2, 1l );

        assertTrue( key.equals( same ) );
        assertTrue( key.hashCode() == same.hashCode() );

        // Similar id
        assertFalse( key.equals( similarId ) );
        assertFalse( key.hashCode() == similarId.hashCode() );

        // Similar property
        assertFalse( key.equals( similarProp ) );
        assertFalse( key.hashCode() == similarProp.hashCode() );
    }
}
