package index.storage;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.pagecache.PagedFile;

public class ByteArrayPagedFile implements PagedFile
{
    private final int pageSize;
    private List<byte[]> pages;

    public ByteArrayPagedFile()
    {
        this( 1024 );
    }

    public ByteArrayPagedFile( int pageSize )
    {
        this.pageSize = pageSize;
        pages = new ArrayList<>();
    }

    @Override
    public ByteArrayCursor io( long pageId, int pf_flags ) throws IOException
    {
        int lockMask = PF_EXCLUSIVE_LOCK | PF_SHARED_LOCK;
        if ( (pf_flags & lockMask) == 0 )
        {
            throw new IllegalArgumentException(
                    "Must specify either PF_EXCLUSIVE_LOCK or PF_SHARED_LOCK" );
        }
        if ( (pf_flags & lockMask) == lockMask )
        {
            throw new IllegalArgumentException(
                    "Cannot specify both PF_EXCLUSIVE_LOCK and PF_SHARED_LOCK" );
        }
        ByteArrayCursor cursor;
        if ( (pf_flags & PF_SHARED_LOCK) == 0 )
        {
            cursor = new ByteArrayWriteCursor();
        }
        else
        {
            cursor = new ByteArrayReadCursor();
        }

        cursor.initialise( this, pageId, pf_flags );
        cursor.rewind();
        return cursor;
    }

    @Override
    public int pageSize()
    {
        return pageSize;
    }

    @Override
    public void flushAndForce() throws IOException
    {
        throw new NotImplementedException();
    }

    @Override
    public long getLastPageId() throws IOException
    {
        return pages.size() - 1;
    }

    @Override
    public void close() throws IOException
    {
        throw new NotImplementedException();
    }

    protected byte[] getPage( long pageId )
    {
        return pages.get( (int) pageId );
    }

    public void increaseLastPageIdTo( long nextPageId )
    {
        while ( pages.size() - 1 < nextPageId )
        {
            pages.add( new byte[pageSize] );
        }
    }
}
