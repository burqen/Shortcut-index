package index.logical;

import org.junit.Test;

import org.neo4j.graphdb.Direction;

import static junit.framework.TestCase.assertNotNull;

public class ShortcutIndexProviderTest
{
    @Test
    public void sameDescription()
    {
        ShortcutIndexProvider provider = new ShortcutIndexProvider();

        ShortcutIndexDescription desc = new ShortcutIndexDescription( "a", "b", "c", Direction.OUTGOING, "d", null );
        ShortcutIndexService index = new ShortcutIndexService( 2, desc );

        provider.put( index );

        assertNotNull( provider.get( desc ) );
    }

    @Test
    public void sameDescriptionContent()
    {
        ShortcutIndexProvider provider = new ShortcutIndexProvider();

        ShortcutIndexDescription in = new ShortcutIndexDescription( "a", "b", "c", Direction.OUTGOING, "d", null );
        ShortcutIndexService index = new ShortcutIndexService( 2, in );

        provider.put( index );

        ShortcutIndexDescription fetch = new ShortcutIndexDescription(  "a", "b", "c", Direction.OUTGOING, "d", null );
        assertNotNull( provider.get( fetch ) );
    }
}
