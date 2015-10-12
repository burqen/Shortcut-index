package index.btree;

import java.util.Arrays;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class TestUtils
{
    protected void assertKey( long[] expected, long[] actual )
    {
        assertTrue( "Expected key to be " + Arrays.toString( expected ) + " but was " + Arrays.toString( actual )
                , Arrays.equals( expected, actual ) );
    }

    protected void assertValue( long[] expected, long[] actual )
    {
        assertTrue( "Expected values to be " + Arrays.toString( expected ) + " but was " + Arrays.toString( actual ),
                Arrays.equals( expected, actual ) );
    }

    protected void assertChild( long expected, long actual )
    {
        assertEquals( "Expected child to be " + expected + " but was actually " + actual, expected, actual );
    }

    protected void assertKeyCount( int expected, int actual )
    {
        assertEquals( "Expected key count to be " + expected + " but was " + actual, expected, actual );
    }


    protected void assertSibling( long expected, long actual )
    {
        assertEquals( "Expected right sibling to be " + expected + " but was " + actual, expected, actual );
    }
}
