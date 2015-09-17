package index.logical;

import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class ShortcutIndexDescriptionTest
{
    @Test
    public void equalsAndHash()
    {
        ShortcutIndexDescription description = new ShortcutIndexDescription( "a" );
        ShortcutIndexDescription same = new ShortcutIndexDescription( "a" );
        ShortcutIndexDescription similar = new ShortcutIndexDescription( "A" );
        ShortcutIndexDescription different = new ShortcutIndexDescription( "icameheartokickassandchewbubblegum" );

        assertTrue( description.equals( same ) );
        assertTrue( description.hashCode() == same.hashCode() );

        // Similar id
        assertFalse( description.equals( similar ) );
        assertFalse( description.hashCode() == similar.hashCode() );

        // Similar property
        assertFalse( description.equals( different ) );
        assertFalse( description.hashCode() == different.hashCode() );
    }
}
