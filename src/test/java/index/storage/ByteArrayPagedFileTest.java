package index.storage;

import org.junit.Before;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;

import org.neo4j.io.pagecache.PagedFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ByteArrayPagedFileTest
{
    ByteArrayPagedFile pagedFile;

    @Before
    public void setup()
    {
        int pageSize = 512;
        pagedFile = new ByteArrayPagedFile( pageSize );
    }

    @Test(expected = NotImplementedException.class)
    public void flushNotImplemented() throws IOException
    {
        pagedFile.flushAndForce();
    }

    @Test(expected = NotImplementedException.class)
    public void closeNotImplemented() throws IOException
    {
        pagedFile.close();
    }

    @Test
    public void increaseLastPageId() throws IOException
    {
        pagedFile.increaseLastPageIdTo( 5 );
        assertEquals( "Expected lastPageId to be 5", 5, pagedFile.getLastPageId() );
    }

    @Test
    public void increaseLastPageIdNegative() throws IOException
    {
        long lastPageId = pagedFile.getLastPageId();
        pagedFile.increaseLastPageIdTo( -1 );
        assertEquals( "Expected lastPageId to be unchanged", lastPageId, pagedFile.getLastPageId() );
    }

    @Test(expected = IllegalArgumentException.class)
    public void ioWithoutFlag() throws IOException
    {
        pagedFile.io( 0, 0 );
    }

    @Test(expected = IllegalArgumentException.class)
    public void ioWithConflictingFlags() throws IOException
    {
        pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK | PagedFile.PF_SHARED_LOCK );
    }

    @Test
    public void ioWithExclusiveLock() throws IOException
    {
        ByteArrayPageCursor cursor = pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK );
        assertTrue( "Expected a write cursor", cursor instanceof ByteArrayWritePageCursor );
    }

    @Test
    public void ioWithSharedLock() throws IOException
    {
        ByteArrayPageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_LOCK );
        assertTrue( "Expected a write cursor", cursor instanceof ByteArrayReadPageCursor );
    }
}
