package index.legacy;

import index.SCResult;

import java.util.List;

public class LegacyBTScanner extends LegacyBTSeeker.LegacyCommonSeeker
{
    @Override
    protected void seekLeaf( LegacyLeafBTreeNode leaf, List<SCResult> resultList )
    {
        while ( leaf != null )
        {
            for ( int i = 0; i < leaf.getKeyCount(); i++ )
            {
                resultList.add( new SCResult( leaf.getKey( i ), leaf.getValue( i ) ) );
            }
            leaf = (LegacyLeafBTreeNode)leaf.getRightSibling();
        }
    }

    @Override
    protected void seekInternal( LegacyInternalBTreeNode internal, List<SCResult> resultList )
    {
        seek( internal.getChild( 0 ), resultList );
    }
}
