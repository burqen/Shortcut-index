package index;

import index.SCValue;
import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class SCValueTest
{
    @Test
    public void equalsAndHash()
    {
        SCValue key = new SCValue( 1, 1l );
        SCValue same = new SCValue( 1, 1l );
        SCValue similarId = new SCValue( 1, 2l );
        SCValue similarProp = new SCValue( 2, 1l );

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
