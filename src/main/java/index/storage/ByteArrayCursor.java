package index.storage;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

/**
 * This cursor should behave like a PageCursor with the exception that it does not point at an offset in a file.
 * It rather points into an isolated byte[]
 */
public class ByteArrayCursor implements PageCursor
{
    private ByteArrayPagedFile pagedFile;
    private long pageId;
    private byte[] page;

    private long nextPageId;
    private long currentPageId;
    private long lastPageId;

    // Offset into page
    private int offset;

    public final void initialize( ByteArrayPagedFile pagedFile, long pageId )
    {
        this.pagedFile = pagedFile;
        this.pageId = pageId;
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
        return 0;
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
        return 0;
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
        return 0l;
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
        return 0l;
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
        return 0;
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
        return 0;
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
        return 0l;
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
        return 0l;
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
        return 0;
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
        return 0;
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
        return 0l;
    }

    /**
     * Get the file page size of the page that the cursor is currently positioned at,
     * or UNBOUND_PAGE_SIZE if next() has not yet been called on this cursor, or returned false.
     * A call to rewind() will make the current page unbound as well, until next() is called.
     */
    @Override
    public int getCurrentPageSize()
    {
        return 0;
    }

    /**
     * Get the file the cursor is currently bound to, or {@code null} if next() has not yet been called on this
     * cursor, or returned false.
     * A call to rewind() will make the cursor unbound as well, until next() is called.
     */
    @Override
    public File getCurrentFile()
    {
        return null;
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
    public boolean next() throws IOException
    {
        return false;
    }

    /**
     * Moves the cursor to the page specified by the given pageId, if any,
     * and returns true when it is ready to be processed. Returns false if
     * for instance, the cursor was requested with PF_NO_GROW and the page
     * most recently processed was the last page in the file.
     */
    @Override
    public boolean next( long pageId ) throws IOException
    {
        return false;
    }

    /**
     * Relinquishes all resources associated with this cursor, including the
     * cursor itself. The cursor cannot be used after this call.
     * @see AutoCloseable#close()
     */
    @Override
    public void close()
    {

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
        return false;
    }
}
