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
            assertTrue( "Expected " + same + " to be equal to " + original, original.equals( same ) );
            assertTrue( "Expected same hashCode from " + same + " and " + original,
                    original.hashCode() == same.hashCode() );
        }

        for ( ShortcutIndexDescription other : shouldNotBeEqual )
        {
            assertFalse( "Expected " + other + " to be not equal to " + original, original.equals( other ) );
            assertFalse( "Expected different hashCodes from " + other + " and " + original,
                    original.hashCode() == other.hashCode() );
        }
    }
}
