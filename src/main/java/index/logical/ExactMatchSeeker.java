package index.logical;

import java.util.List;

public class ExactMatchSeeker extends BTSeeker.CommonSeeker
{
    private final TKey matchKey;

    public ExactMatchSeeker( TKey matchKey )
    {
        this.matchKey = matchKey;
    }

    protected void seekInternal( InternalBTreeNode internal, List<TResult> resultList )
    {
        BTreeNode child = null;

        for ( int i = 0; i < internal.getKeyCount(); i++ )
        {
            TKey key = internal.getKey( i );
            if ( key.compareTo( matchKey ) > 0 )
            {
                child = internal.getChild( i );
                break;
            }
        }
        if ( child == null )
        {
            child = internal.getChild( internal.getKeyCount() );
        }

        seek( child, resultList );
    }

    protected void seekLeaf( LeafBTreeNode leaf, List<TResult> resultList )
    {
        for ( int i = 0; i < leaf.getKeyCount(); i++ )
        {
            TKey key = leaf.getKey( i );
            if ( key.compareTo( matchKey ) == 0 )
            {
                resultList.add( new TResult( key, leaf.getValue( i ) ) );
            }
            else if ( key.compareTo( matchKey ) > 0 )
            {
                return;
            }
        }
        LeafBTreeNode rightSibling = (LeafBTreeNode)leaf.getRightSibling();
        if ( rightSibling != null )
        {
            seekLeaf( rightSibling, resultList );
        }
    }
}
