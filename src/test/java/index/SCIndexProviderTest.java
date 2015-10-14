package index;

import index.btree.Index;
import index.legacy.LegacyIndex;
import index.storage.ByteArrayPagedFile;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;

import org.neo4j.graphdb.Direction;
import org.neo4j.io.pagecache.PagedFile;

import static junit.framework.TestCase.assertNotNull;

public class SCIndexProviderTest
{
    private PagedFile pagedFile;

    @Before
    public void setup()
    {
        pagedFile = new ByteArrayPagedFile( 32 );
    }

    @Test
    public void sameDescription() throws IOException
    {
        SCIndexProvider provider = new SCIndexProvider();

        SCIndexDescription desc = new SCIndexDescription( "a", "b", "c", Direction.OUTGOING, "d", null );
        SCIndex index = new Index( pagedFile, desc );

        provider.put( index );

        assertNotNull( provider.get( desc ) );
    }

    @Test
    public void sameDescriptionContent() throws IOException
    {
        SCIndexProvider provider = new SCIndexProvider();

        SCIndexDescription in = new SCIndexDescription( "a", "b", "c", Direction.OUTGOING, "d", null );
        SCIndex index = new Index( pagedFile, in );

        provider.put( index );

        SCIndexDescription fetch = new SCIndexDescription(  "a", "b", "c", Direction.OUTGOING, "d", null );
        assertNotNull( provider.get( fetch ) );
    }
}
