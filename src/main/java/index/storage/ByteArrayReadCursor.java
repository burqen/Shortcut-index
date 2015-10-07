package index.storage;

import java.io.IOException;

public class ByteArrayReadCursor extends ByteArrayCursor
{
    /**
     * Moves the cursor to the next page, if any, and returns true when it is
     * ready to be processed. Returns false if there are no more pages to be
     * processed. For instance, if the cursor was requested with PF_NO_GROW
     * and the page most recently processed was the last page in the file.
     */
    @Override
    public boolean next() throws IOException
    {
        if ( nextPageId > lastPageId )
        {
            return false;
        }
        pin( nextPageId );
        currentPageId = nextPageId;
        nextPageId++;
        return true;
    }

    // Remove put methods

    @Override
    public void putByte( byte value )
    {
        throw new IllegalStateException( "Cannot write to read-locked page" );
    }

    @Override
    public void putLong( long value )
    {
        throw new IllegalStateException( "Cannot write to read-locked page" );
    }

    @Override
    public void putInt( int value )
    {
        throw new IllegalStateException( "Cannot write to read-locked page" );
    }

    @Override
    public void putBytes( byte[] data, int arrayOffset, int length )
    {
        throw new IllegalStateException( "Cannot write to read-locked page" );
    }

    @Override
    public void putShort( short value )
    {
        throw new IllegalStateException( "Cannot write to read-locked page" );
    }
}
