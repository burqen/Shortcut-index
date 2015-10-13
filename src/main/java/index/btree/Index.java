package index.btree;

import index.SCIndex;
import index.SCIndexDescription;
import index.legacy.BTSeeker;
import index.legacy.TResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class Index implements SCIndex, IdProvider
{
    private PagedFile pagedFile;
    private SCIndexDescription description;
    private long rootId;
    private IndexInsert inserter;
    private IdPool idPool;
    private Node node;

    public Index( PagedFile pagedFile, SCIndexDescription description ) throws IOException
    {
        this( pagedFile, description, 0 );
    }

    public Index( PagedFile pagedFile, SCIndexDescription description, int rootId ) throws IOException
    {
        this.pagedFile = pagedFile;
        this.description = description;
        this.rootId = rootId;
        this.node = new Node( pagedFile.pageSize() );
        this.inserter = new IndexInsert( this, node );
        this.idPool = new IdPool();

        // Initialize index root node to a leaf node. This should be changed when moving to persistent index.
        PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_EXCLUSIVE_LOCK );
        cursor.next();
        node.initializeLeaf( cursor );
    }

    @Override
    public SCIndexDescription getDescription()
    {
        return description;
    }

    @Override
    public void insert( long[] key, long[] value ) throws IOException
    {
        PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_EXCLUSIVE_LOCK );
        cursor.next();

        SplitResult split = inserter.insert( cursor, key, value );

        if ( split != null )
        {
            // New root
            rootId = idPool.acquireNewId();
            cursor.next( rootId );

            node.initializeInternal( cursor );
            node.setKeyAt( cursor, split.primKey, 0 );
            node.setKeyCount( cursor, 1 );
            node.setChildAt( cursor, split.left, 0 );
            node.setChildAt( cursor, split.right, 1 );
        }
    }

    public void seek( Seeker seeker, List<TResult> resultList ) throws IOException
    {
        PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_EXCLUSIVE_LOCK );
        cursor.next();

        seeker.seek( cursor, resultList );
    }

    public long acquireNewId()
    {
        return idPool.acquireNewId();
    }
}
