package index.legacy;

import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class TResultTest
{
    @Test
    public void equalsAndHash()
    {
        TResult result = new TResult( new TKey( 1, 1 ), new TValue( 1, 1 ) );
        TResult same = new TResult( new TKey( 1, 1 ), new TValue( 1, 1 ) );
        TResult similarKey = new TResult( new TKey( 1, 1 ), new TValue( 2, 2 ) );
        TResult similarValue = new TResult( new TKey( 2, 2 ), new TValue( 1, 1 ) );

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
