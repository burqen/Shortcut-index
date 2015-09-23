package index.logical;

import java.util.List;

public class BTScanner extends BTSeeker.CommonSeeker
{
    @Override
    protected void seekLeaf( LeafBTreeNode leaf, List<TResult> resultList )
    {
        while ( leaf != null )
        {
            for ( int i = 0; i < leaf.getKeyCount(); i++ )
            {
                resultList.add( new TResult( leaf.getKey( i ), leaf.getValue( i ) ) );
            }
            leaf = (LeafBTreeNode)leaf.getRightSibling();
        }
    }

    @Override
    protected void seekInternal( InternalBTreeNode internal, List<TResult> resultList )
    {
        seek( internal.getChild( 0 ), resultList );
    }
}
