package index.storage;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class ByteArrayPagedFile implements PagedFile
{

    @Override
    public PageCursor io( long l, int i ) throws IOException
    {
        return null;
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
