package index.legacy;

import index.SCIndexDescription;
import index.ShortcutIndexProvider;
import index.SCIndex;
import org.junit.Test;

import org.neo4j.graphdb.Direction;

import static junit.framework.TestCase.assertNotNull;

public class ShortcutIndexProviderTest
{
    @Test
    public void sameDescription()
    {
        ShortcutIndexProvider provider = new ShortcutIndexProvider();

        SCIndexDescription desc = new SCIndexDescription( "a", "b", "c", Direction.OUTGOING, "d", null );
        LegacySCIndex index = new LegacySCIndex( 2, desc );

        provider.put( index );

        assertNotNull( provider.get( desc ) );
    }

    @Test
    public void sameDescriptionContent()
    {
        ShortcutIndexProvider provider = new ShortcutIndexProvider();

        SCIndexDescription in = new SCIndexDescription( "a", "b", "c", Direction.OUTGOING, "d", null );
        LegacySCIndex index = new LegacySCIndex( 2, in );

        provider.put( index );

        SCIndexDescription fetch = new SCIndexDescription(  "a", "b", "c", Direction.OUTGOING, "d", null );
        assertNotNull( provider.get( fetch ) );
    }
}
