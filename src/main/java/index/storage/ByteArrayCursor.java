package index.storage;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

/**
 * This cursor should behave like a PageCursor with the exception that it does not point at an offset in a file.
 * It rather points into an isolated byte[]
 */
public abstract class ByteArrayCursor implements PageCursor
{
    protected ByteArrayPagedFile pagedFile;
    protected long pageId;
    protected byte[] page;

    protected long nextPageId;
    protected long currentPageId;
    protected long lastPageId;

    // Offset into page
    protected int offset;
    protected int pf_flags;

    public final void initialise( ByteArrayPagedFile pagedFile, long pageId, int pf_flags )
    {
        this.pagedFile = pagedFile;
        this.pageId = pageId;
        this.pf_flags = pf_flags;
    }

    /**
     * Get the signed byte at the current page offset, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    @Override
    public byte getByte()
    {
        byte b = page[offset];
        offset++;
        return b;
    }

    /**
     * Get the signed byte at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    @Override
    public byte getByte( int offset )
    {
        return page[offset];
    }

    /**
     * Set the signed byte at the current offset into the page, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    @Override
    public void putByte( byte value )
    {
        page[offset] = value;
        offset++;
    }

    /**
     * Set the signed byte at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    @Override
    public void putByte( int offset, byte value )
    {
        page[offset] = value;
    }

    /**
     * Get the signed long at the current page offset, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    @Override
    public long getLong()
    {
        long l = getLong( offset );
        offset += 8;

        return l;
    }

    /**
     * Get the signed long at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    @Override
    public long getLong( int offset )
    {
        long a = page[offset    ] & 0xFF;
        long b = page[offset + 1] & 0xFF;
        long c = page[offset + 2] & 0xFF;
        long d = page[offset + 3] & 0xFF;
        long e = page[offset + 4] & 0xFF;
        long f = page[offset + 5] & 0xFF;
        long g = page[offset + 6] & 0xFF;
        long h = page[offset + 7] & 0xFF;

        return (a << 56) | (b << 48) | (c << 40) | (d << 32) | (e << 24) | (f << 16) | (g << 8) | h;
    }

    /**
     * Set the signed long at the current offset into the page, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    @Override
    public void putLong( long value )
    {
        putLong( offset, value );
        offset += 8;
    }

    /**
     * Set the signed long at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    @Override
    public void putLong( int offset, long value )
    {
        page[offset    ] = (byte)( value >> 56 );
        page[offset + 1] = (byte)( value >> 48 );
        page[offset + 2] = (byte)( value >> 40 );
        page[offset + 3] = (byte)( value >> 32 );
        page[offset + 4] = (byte)( value >> 24 );
        page[offset + 5] = (byte)( value >> 16 );
        page[offset + 6] = (byte)( value >> 8  );
        page[offset + 7] = (byte)( value       );
    }

    /**
     * Get the signed int at the current page offset, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    @Override
    public int getInt()
    {
        int i = getInt( offset );
        offset += 4;

        return i;
    }

    /**
     * Get the signed int at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    @Override
    public int getInt( int offset )
    {
        int a = page[offset    ] & 0xFF;
        int b = page[offset + 1] & 0xFF;
        int c = page[offset + 2] & 0xFF;
        int d = page[offset + 3] & 0xFF;

        return (a << 24) | (b << 16) | (c << 8) | d;
    }

    /**
     * Set the signed int at the current offset into the page, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    @Override
    public void putInt( int value )
    {
        putInt( offset, value );
        offset += 4;
    }

    /**
     * Set the signed int at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    @Override
    public void putInt( int offset, int value )
    {
        page[offset    ] = (byte)( value >> 24 );
        page[offset + 1] = (byte)( value >> 16 );
        page[offset + 2] = (byte)( value >> 8  );
        page[offset + 3] = (byte)( value       );
    }

    /**
     * Get the unsigned int at the current page offset, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    @Override
    public long getUnsignedInt()
    {
        return getInt() & 0xFFFFFFFFL;
    }

    /**
     * Get the unsigned int at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    @Override
    public long getUnsignedInt( int offset )
    {
        return getInt( offset ) & 0xFFFFFFFFL;
    }

    /**
     * Fill the given array with bytes from the page, beginning at the current offset into the page,
     * and then increment the current offset by the length of the array.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset plus the length of the array reaches beyond the end of the page.
     */
    @Override
    public void getBytes( byte[] data )
    {
        getBytes( data, 0, data.length );
    }

    /**
     * Read the given length of bytes from the page into the given array, starting from the current offset into the
     * page, and writing from the given array offset, and then increment the current offset by the length argument.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset plus the length reaches beyond the end of the page.
     */
    @Override
    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        System.arraycopy( page, offset, data, arrayOffset, length );
        offset += length;
    }

    /**
     * Write out all the bytes of the given array into the page, beginning at the current offset into the page,
     * and then increment the current offset by the length of the array.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset plus the length of the array reaches beyond the end of the page.
     */
    @Override
    public void putBytes( byte[] data )
    {
        putBytes( data, 0, data.length );
    }

    /**
     * Write out the given length of bytes from the given offset into the the given array of bytes, into the page,
     * beginning at the current offset into the page, and then increment the current offset by the length argument.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset plus the length reaches beyond the end of the page.
     */
    @Override
    public void putBytes( byte[] data, int arrayOffset, int length )
    {
        System.arraycopy( data, arrayOffset, page, offset, length );
        offset += length;
    }

    /**
     * Get the signed short at the current page offset, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    @Override
    public short getShort()
    {
        short s = getShort( offset );
        offset += 2;
        return s;
    }

    /**
     * Get the signed short at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    @Override
    public short getShort( int offset )
    {
        short a = (short) (page[ offset     ] & 0xFF);
        short b = (short) (page[ offset + 1 ] & 0xFF);
        return (short) ((a << 8) | b);
    }

    /**
     * Set the signed short at the current offset into the page, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    @Override
    public void putShort( short value )
    {
        putShort( offset, value );
        offset += 2;
    }

    /**
     * Set the signed short at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    @Override
    public void putShort( int offset, short value )
    {
        page[ offset    ] = (byte)( value >> 8 );
        page[ offset + 1] = (byte)( value   );
    }

    /**
     * Set the current offset into the page, for interacting with the page through the read and write methods that do
     * not take a specific offset as an argument.
     */
    @Override
    public void setOffset( int offset )
    {
        if ( offset < 0 )
        {
            throw new IndexOutOfBoundsException();
        }
        this.offset = offset;
    }

    /**
     * Get the current offset into the page, which is the location on the page where the next interaction would take
     * place through the read and write methods that do not take a specific offset as an argument.
     */
    @Override
    public int getOffset()
    {
        return offset;
    }

    /**
     * Get the file page id that the cursor is currently positioned at, or
     * UNBOUND_PAGE_ID if next() has not yet been called on this cursor, or returned false.
     * A call to rewind() will make the current page id unbound as well, until
     * next() is called.
     */
    @Override
    public long getCurrentPageId()
    {
        return currentPageId;
    }

    /**
     * Get the file page size of the page that the cursor is currently positioned at,
     * or UNBOUND_PAGE_SIZE if next() has not yet been called on this cursor, or returned false.
     * A call to rewind() will make the current page unbound as well, until next() is called.
     */
    @Override
    public int getCurrentPageSize()
    {
        return currentPageId == UNBOUND_PAGE_ID?
               UNBOUND_PAGE_SIZE : pagedFile.pageSize();
    }

    /**
     * Get the file the cursor is currently bound to, or {@code null} if next() has not yet been called on this
     * cursor, or returned false.
     * A call to rewind() will make the cursor unbound as well, until next() is called.
     */
    @Override
    public File getCurrentFile()
    {
        throw new NotImplementedException();
    }

    /**
     * Rewinds the cursor to its initial condition, as if freshly returned from
     * an equivalent io() call. In other words, the next call to next() will
     * move the cursor to the starting page that was specified in the io() that
     * produced the cursor.
     * @throws IOException
     */
    @Override
    public void rewind() throws IOException
    {
        nextPageId = pageId;
        currentPageId = UNBOUND_PAGE_ID;
        lastPageId = pagedFile.getLastPageId();
    }

    /**
     * Moves the cursor to the next page, if any, and returns true when it is
     * ready to be processed. Returns false if there are no more pages to be
     * processed. For instance, if the cursor was requested with PF_NO_GROW
     * and the page most recently processed was the last page in the file.
     */
    @Override
    public abstract boolean next() throws IOException;

    /**
     * Moves the cursor to the page specified by the given pageId, if any,
     * and returns true when it is ready to be processed. Returns false if
     * for instance, the cursor was requested with PF_NO_GROW and the page
     * most recently processed was the last page in the file.
     */
    @Override
    public boolean next( long pageId ) throws IOException
    {
        nextPageId = pageId;
        return next();
    }

    protected void pin( long nextPageId )
    {
        byte[] page = pagedFile.getPage( nextPageId );
        reset( page );
    }

    public final void reset( byte[] page )
    {
        this.page = page;
        this.offset = 0;
    }

    /**
     * Relinquishes all resources associated with this cursor, including the
     * cursor itself. The cursor cannot be used after this call.
     * @see AutoCloseable#close()
     */
    @Override
    public void close()
    {
        throw new NotImplementedException();
    }

    /**
     * Returns true if the page has entered an inconsistent state since the
     * last call to next() or shouldRetry().
     * If this method returns true, the in-page offset of the cursor will be
     * reset to zero.
     *
     * @throws IOException If the page was evicted while doing IO, the cursor will have
     *                     to do a page fault to get the page back.
     *                     This may throw an IOException.
     */
    @Override
    public boolean shouldRetry() throws IOException
    {
        throw new NotImplementedException();
    }
}
