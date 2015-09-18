package index.logical;

import org.junit.Test;

import org.neo4j.graphdb.Direction;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class ShortcutIndexDescriptionTest
{
    private String firstLabel = "label1";
    private String secondLabel = "label2";
    private String relType = "reltype";
    private String prop = "prop";
    private Direction dir = Direction.OUTGOING;
    private String other = "other";

    @Test
    public void equalsAndHash()
    {
        ShortcutIndexDescription original = new ShortcutIndexDescription(
                firstLabel, secondLabel, relType, dir, prop, null );

        ShortcutIndexDescription[] shouldBeEqual = new ShortcutIndexDescription[]
                {
                        new ShortcutIndexDescription( firstLabel, secondLabel, relType, dir, prop, null ),
                        new ShortcutIndexDescription( firstLabel.toUpperCase(), secondLabel.toUpperCase(),
                                relType.toUpperCase(), dir, prop.toUpperCase(), null ),
                };

        ShortcutIndexDescription[] shouldNotBeEqual = new ShortcutIndexDescription[]
                {
                        new ShortcutIndexDescription( firstLabel, secondLabel, relType, dir, null, prop ),
                        new ShortcutIndexDescription( other, secondLabel, relType, dir, prop, null ),
                        new ShortcutIndexDescription( firstLabel, other, relType, dir, prop, null ),
                        new ShortcutIndexDescription( firstLabel, secondLabel, other, dir, prop, null ),
                        new ShortcutIndexDescription( firstLabel, secondLabel, relType, dir, other, null ),
                        new ShortcutIndexDescription( firstLabel, secondLabel, relType, dir.reverse(), prop, null ),
                };

        for ( ShortcutIndexDescription same : shouldBeEqual )
        {
            assertTrue( original.equals( same ) );
            assertTrue( original.hashCode() == same.hashCode() );
        }

        for ( ShortcutIndexDescription other : shouldNotBeEqual )
        {
            assertFalse( original.equals( other ) );
            assertFalse( original.hashCode() == other.hashCode() );
        }
    }
}
