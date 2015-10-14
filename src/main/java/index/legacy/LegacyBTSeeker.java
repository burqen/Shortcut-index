package index.legacy;

import index.SCResult;

import java.util.List;


public interface LegacyBTSeeker
{
    void seek( LegacyBTreeNode node, List<SCResult> resultList );

    public abstract class LegacyCommonSeeker implements LegacyBTSeeker
    {
        public void seek( LegacyBTreeNode node, List<SCResult> resultList )
        {
            if ( node.getNodeType() == LegacyBTreeNode.BTreeNodeType.InternalNode )
            {
                LegacyInternalBTreeNode internal = (LegacyInternalBTreeNode)node;
                seekInternal( internal, resultList );
            }
            else if ( node.getNodeType() == LegacyBTreeNode.BTreeNodeType.LeafNode )
            {
                LegacyLeafBTreeNode leaf = (LegacyLeafBTreeNode)node;

                seekLeaf( leaf, resultList );
            }
            else
            {
                throw new IllegalStateException( "node reported type other than InternalNode or LeafNode" );
            }
        }

        protected abstract void seekLeaf( LegacyLeafBTreeNode leaf, List<SCResult> resultList );

        protected abstract void seekInternal( LegacyInternalBTreeNode internal, List<SCResult> resultList );
    }
}
