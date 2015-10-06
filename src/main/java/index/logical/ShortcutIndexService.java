package index.logical;

import index.storage.ByteArrayCursor;
import index.storage.ByteArrayPagedFile;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

public class ShortcutIndexService
{
    private ByteArrayPagedFile pagedFile;
    private ByteArrayCursor cursor;
    private final ShortcutIndexDescription description;
    private long rootId = 0;

    private BTreeNode root;

    public ShortcutIndexService( int order, ShortcutIndexDescription description ) throws IOException
    {
        int pageSize = 1024;
        int maxSizeInMB = 512;
        int nbrOfPages = maxSizeInMB * ( 1000000 / pageSize );

        pagedFile = new ByteArrayPagedFile();
        cursor = pagedFile.io( rootId, 0 );

        this.description = description;
        root = new LeafBTreeNode( order );
    }

    public ShortcutIndexDescription getDescription()
    {
        return description;
    }

    public void insert( TKey key, TValue value )
    {
        root.insert( key.getId(), key.getProp(), value );

        // Will only enter loop when split has occurred in root
        while ( root.getParent() != null )
        {
            root = root.getParent();
        }
    }

    public void seek( BTSeeker seeker, List<TResult> list )
    {
        seeker.seek( root, list );
    }

    // Methods mainly for testing

    public long totalKeyCount()
    {
        return root.totalKeyCount();
    }

    public int height()
    {
        return root.height();
    }

    public void printTree( PrintStream out )
    {
        root.printTree( out );
    }
}
