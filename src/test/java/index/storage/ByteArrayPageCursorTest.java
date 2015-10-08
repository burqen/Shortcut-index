package index.storage;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.io.pagecache.PagedFile;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class ByteArrayPageCursorTest
{
    private int pageSize = 32;
    private ByteArrayPagedFile pagedFile;
    private ByteArrayPageCursor cursor;

    @Before
    public void setup()
    {
        pagedFile = new ByteArrayPagedFile( pageSize );
    }

    // WRITE CURSOR

    @Test
    public void readWriteByte() throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK );
        assertTrue( "Expected to have next", cursor.next() );
        byte value = 111;
        cursor.putByte( value );
        cursor.setOffset( cursor.getOffset() - 1 );
        byte b = cursor.getByte();
        assertEquals( "Expected byte to be " + value + " but was " + b, value, b );
    }

    @Test
    public void readWriteShort() throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK );
        assertTrue( "Expected to have next", cursor.next() );
        short value = 32111;
        cursor.putShort( value );
        cursor.setOffset( cursor.getOffset() - 2 );
        short s = cursor.getShort();
        assertEquals( "Expected short to be " + value + " but was " + s, value, s );
    }

    @Test
    public void readWriteInt() throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK );
        assertTrue( "Expected to have next", cursor.next() );
        int value = Integer.MAX_VALUE - 1;
        cursor.putInt( value );
        cursor.setOffset( cursor.getOffset() - 4 );
        int i = cursor.getInt();
        assertEquals( "Expected int to be " + value + " but was " + i, value, i );
    }

    @Test
    public void readWriteLong() throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK );
        assertTrue( "Expected to have next", cursor.next() );
        long value = Long.MAX_VALUE - 1;
        cursor.putLong( value );
        cursor.setOffset( cursor.getOffset() - 8 );
        long l = cursor.getLong();
        assertEquals( "Expected long to be " + value + " but was " + l, value, l );
    }

    @Test
    public void ioNotStartingOnPageZero() throws IOException
    {
        cursor = pagedFile.io( 1, PagedFile.PF_EXCLUSIVE_LOCK );
        assertTrue( "Expected cursor to have next.", cursor.next() );
        long currentPageId = cursor.getCurrentPageId();
        assertEquals( "Expected current pageId to be 1 but was " + currentPageId,
                1, currentPageId );
    }

    @Test
    public void nextPage() throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK );
        assertTrue( "Expected cursor to have next.", cursor.next() );
        assertTrue( "Expected cursor to have next.", cursor.next() );
        long currentPageId = cursor.getCurrentPageId();
        assertEquals( "Expected current page id to be 1 but was " + currentPageId, 1, currentPageId );
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void putOutsidePageSize() throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK );
        cursor.next();
        byte b = 0;
        cursor.putByte( cursor.getCurrentPageSize(), b );
    }

    @Test
    public void readWriteBytes() throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK );
        cursor.next();
        byte[] writeBytes = new byte[pageSize];
        ByteBuffer bb = ByteBuffer.wrap( writeBytes );
        {
            for ( byte i = 0; i < pageSize; i ++ )
            {
                bb.put( i );
            }
        }
        writeBytes = bb.array();
        cursor.putBytes( writeBytes );
        cursor = pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK );
        cursor.next();
        byte[] readBytes = new byte[pageSize];
        cursor.getBytes( readBytes );
        assertArrayEquals( "Expected readBytes to contain same data as writeBytes but was different. " +
                           "\nwriteBytes: " + Arrays.toString( writeBytes ) +
                           "\n readBytes: " + Arrays.toString( readBytes )  ,
                writeBytes, readBytes );
    }


    // READ CURSOR

    @Test(expected = IllegalStateException.class)
    public void tryPutByte() throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_SHARED_LOCK );
        cursor.next();
        byte b = 0;
        cursor.putByte( b );
    }

    @Test(expected = IllegalStateException.class)
    public void tryPutShort() throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_SHARED_LOCK );
        cursor.next();
        short s = 0;
        cursor.putShort( s );
    }

    @Test(expected = IllegalStateException.class)
    public void tryPutInt() throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_SHARED_LOCK );
        cursor.next();
        int i = 0;
        cursor.putInt( i );
    }

    @Test(expected = IllegalStateException.class)
    public void tryPutLong() throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_SHARED_LOCK );
        cursor.next();
        long l = 0;
        cursor.putLong( l );
    }

    @Test(expected = IllegalStateException.class)
    public void tryPutBytes() throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_SHARED_LOCK );
        cursor.next();
        byte[] bytes = new byte[0];
        cursor.putBytes( bytes );
    }

    @Test
    public void nextOnReadCursor() throws IOException
    {
        cursor = pagedFile.io( 0, PagedFile.PF_SHARED_LOCK );
        cursor.next();
        assertFalse( "Did not expect cursor to have a next", cursor.next() );
    }

    @Test
    public void nextOnReadCursorOK() throws IOException
    {
        pagedFile.increaseLastPageIdTo( 1 );
        cursor = pagedFile.io( 0, PagedFile.PF_SHARED_LOCK );
        assertTrue( "Expected cursor to have access to first", cursor.next() );
        assertTrue( "Expected cursor to have access to second", cursor.next() );
        assertFalse( "Did not expected cursor to have access to third", cursor.next() );
    }
}
