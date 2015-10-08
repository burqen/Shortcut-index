package index.btree;

import index.SCIndex;
import index.SCIndexDescription;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class Index implements SCIndex
{
    private PagedFile pagedFile;
    private SCIndexDescription description;
    private long rootId;

    public Index( PagedFile pagedFile, SCIndexDescription description )
    {
        this( pagedFile, description, 0 );
    }

    public Index( PagedFile pagedFile, SCIndexDescription description, int rootId )
    {
        this.pagedFile = pagedFile;
        this.description = description;
        this.rootId = rootId;
    }

    @Override
    public SCIndexDescription getDescription()
    {
        return description;
    }

    public void insert( long[] key, long[] value ) throws IOException
    {
        PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_EXCLUSIVE_LOCK );
        cursor.next();

        SplitResult split = IndexInsert.insert( cursor, key, value );

        if ( split != null )
        {
            // New root

        }
    }
}
