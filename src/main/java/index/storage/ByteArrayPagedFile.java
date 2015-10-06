package index.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.pagecache.PagedFile;

public class ByteArrayPagedFile implements PagedFile
{
    public static final int PAGE_SIZE = 1024;
    private List<byte[]> pages;

    public ByteArrayPagedFile()
    {
        pages = new ArrayList<>();
    }

    @Override
    public ByteArrayCursor io( long pageId, int pf_flags ) throws IOException
    {
        // Check flags. Don't have any yet.

        ByteArrayCursor cursor = new ByteArrayCursor();
        cursor.initialize( this, pageId );
        cursor.rewind();
        return cursor;
    }

    @Override
    public int pageSize()
    {
        return 0;
    }

    @Override
    public void flushAndForce() throws IOException
    {

    }

    @Override
    public long getLastPageId() throws IOException
    {
        return 0;
    }

    @Override
    public void close() throws IOException
    {

    }
}
