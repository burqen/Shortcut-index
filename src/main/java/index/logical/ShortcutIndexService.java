package index.logical;

public class ShortcutIndexService
{
    private BTreeNode root;

    ShortcutIndexService( int order )
    {
        root = new LeafBTreeNode( order );
    }

    public void insert( TKey key, TValue value )
    {
        root.insert( key, value );
    }
}
