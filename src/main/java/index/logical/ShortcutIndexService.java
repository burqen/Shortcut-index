package index.logical;

import java.io.PrintStream;
import java.util.List;

public class ShortcutIndexService
{
    private final ShortcutIndexDescription description;

    private BTreeNode root;

    public ShortcutIndexService( int order, ShortcutIndexDescription description )
    {

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
