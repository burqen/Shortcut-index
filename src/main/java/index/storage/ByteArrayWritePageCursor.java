package index.storage;

import java.io.IOException;

import org.neo4j.io.pagecache.PagedFile;

public class ByteArrayWritePageCursor extends ByteArrayPageCursor
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
            if ( (pf_flags & PagedFile.PF_NO_GROW) != 0 )
            {
                return false;
            }
            else
            {
                pagedFile.increaseLastPageIdTo( nextPageId );
            }
        }
        pin( nextPageId );
        currentPageId = nextPageId;
        nextPageId++;
        return true;
    }
}
