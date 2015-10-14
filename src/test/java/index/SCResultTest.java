package index;

import index.SCKey;
import index.SCResult;
import index.SCValue;
import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class SCResultTest
{
    @Test
    public void equalsAndHash()
    {
        SCResult result = new SCResult( new SCKey( 1, 1 ), new SCValue( 1, 1 ) );
        SCResult same = new SCResult( new SCKey( 1, 1 ), new SCValue( 1, 1 ) );
        SCResult similarKey = new SCResult( new SCKey( 1, 1 ), new SCValue( 2, 2 ) );
        SCResult similarValue = new SCResult( new SCKey( 2, 2 ), new SCValue( 1, 1 ) );

        assertTrue( result.equals( same ) );
        assertTrue( result.hashCode() == same.hashCode() );

        // Similar key
        assertFalse( result.equals( similarKey ) );
        assertFalse( result.hashCode() == similarKey.hashCode() );

        // Similar value
        assertFalse( result.equals( similarValue ) );
        assertFalse( result.hashCode() == similarValue.hashCode() );
    }
}
