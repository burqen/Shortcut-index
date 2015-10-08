package index.btree;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.io.pagecache.PageCursor;

@RunWith( Parameterized.class )
public class IndexSearchTest
{
    @Test
    public void searchNoKeys()
    {
        // CONTINUE HERE!
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        ByteBuffer buffer = ByteBuffer.allocate( 128 );
        PageCursor leafCursor = new ByteBufferCursor( buffer );
        Node.initializeLeaf( leafCursor );

        buffer = ByteBuffer.allocate( 128 );
        PageCursor internalCursor = new ByteBufferCursor( buffer );
        Node.initializeInternal( internalCursor );
        return Arrays.asList( new Object[][]
                {
                        {
                            leafCursor
                        },
                        {
                            internalCursor
                        }
                } );
    }

    private PageCursor cursor;

    public IndexSearchTest( PageCursor cursor )
    {
        this.cursor = cursor;
    }
}
