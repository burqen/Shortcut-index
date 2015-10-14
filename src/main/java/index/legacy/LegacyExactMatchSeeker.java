package index.legacy;

import index.SCKey;
import index.SCResult;

import java.util.List;

public class LegacyExactMatchSeeker extends LegacyBTSeeker.LegacyCommonSeeker
{
    private final SCKey matchKey;

    public LegacyExactMatchSeeker( SCKey matchKey )
    {
        this.matchKey = matchKey;
    }

    protected void seekInternal( LegacyInternalBTreeNode internal, List<SCResult> resultList )
    {
        LegacyBTreeNode child = null;

        for ( int i = 0; i < internal.getKeyCount(); i++ )
        {
            SCKey key = internal.getKey( i );
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

    protected void seekLeaf( LegacyLeafBTreeNode leaf, List<SCResult> resultList )
    {
        for ( int i = 0; i < leaf.getKeyCount(); i++ )
        {
            SCKey key = leaf.getKey( i );
            if ( key.compareTo( matchKey ) == 0 )
            {
                resultList.add( new SCResult( key, leaf.getValue( i ) ) );
            }
            else if ( key.compareTo( matchKey ) > 0 )
            {
                return;
            }
        }
        LegacyLeafBTreeNode rightSibling = (LegacyLeafBTreeNode)leaf.getRightSibling();
        if ( rightSibling != null )
        {
            seekLeaf( rightSibling, resultList );
        }
    }
}
