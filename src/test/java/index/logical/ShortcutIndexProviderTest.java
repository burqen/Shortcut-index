package index.logical;

import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;

public class ShortcutIndexProviderTest
{
    @Test
    public void sameDescription()
    {
        ShortcutIndexProvider provider = new ShortcutIndexProvider();

        ShortcutIndexDescription desc = new ShortcutIndexDescription( "index" );
        ShortcutIndexService index = new ShortcutIndexService( 2, desc );

        provider.put( index );

        assertNotNull( provider.get( desc ) );
    }

    @Test
    public void sameDescriptionContent()
    {
        ShortcutIndexProvider provider = new ShortcutIndexProvider();

        ShortcutIndexDescription insertion = new ShortcutIndexDescription( "index" );
        ShortcutIndexService index = new ShortcutIndexService( 2, insertion );

        provider.put( index );

        ShortcutIndexDescription fetch = new ShortcutIndexDescription( "index" );
        assertNotNull( provider.get( fetch ) );
    }
}
