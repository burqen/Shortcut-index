package index.btree;

import index.IdProvider;
import index.SCIndex;
import index.SCIndexDescription;
import index.SCResult;
import index.Seeker;

import java.io.IOException;
import java.util.Arrays;
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
        this.pagedFile = pagedFile;
        this.description = description;
        this.idPool = new IdPool();
        this.rootId = this.idPool.acquireNewId();
        this.node = new Node( pagedFile.pageSize() );
        this.inserter = new IndexInsert( this, node );


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

    public void seek( Seeker seeker, List<SCResult> resultList ) throws IOException
    {
        PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_EXCLUSIVE_LOCK );
        cursor.next();

        seeker.seek( cursor, node, resultList );
    }

    public long acquireNewId()
    {
        return idPool.acquireNewId();
    }

    public void printTree() throws IOException
    {
        PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_LOCK );
        cursor.next();

        int level = 0;
        long id;
        while ( node.isInternal( cursor ) )
        {
            System.out.println( "Level " + level++ );
            id = cursor.getCurrentPageId();
            printKeysOfSiblings( cursor, node );
            System.out.println();
            cursor.next( id );
            cursor.next( node.childAt( cursor, 0 ) );
        }

        System.out.println( "Level " + level );
        printKeysOfSiblings( cursor, node );
        System.out.println();
    }

    protected static void printKeysOfSiblings( PageCursor cursor, Node node ) throws IOException
    {
        while ( true )
        {
            printKeys( cursor, node );
            long rightSibling = node.rightSibling( cursor );
            if ( rightSibling == Node.NO_NODE_FLAG )
            {
                break;
            }
            cursor.next( rightSibling );
        }
    }

    protected static void printKeys( PageCursor cursor, Node node )
    {
        int keyCount = node.keyCount( cursor );
        System.out.print( "|" );
        for ( int i = 0; i < keyCount; i++ )
        {
            System.out.print( Arrays.toString( node.keyAt( cursor, i ) ) + " " );
        }
        System.out.print( "|" );
    }
}
