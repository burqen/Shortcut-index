package index.logical;

import java.util.List;


public interface BTSeeker
{
    void seek( BTreeNode node, List<TResult> resultList );

    public abstract class CommonSeeker implements BTSeeker
    {
        public void seek( BTreeNode node, List<TResult> resultList )
        {
            if ( node.getNodeType() == BTreeNode.BTreeNodeType.InternalNode )
            {
                InternalBTreeNode internal = (InternalBTreeNode)node;
                seekInternal( internal, resultList );
            }
            else if ( node.getNodeType() == BTreeNode.BTreeNodeType.LeafNode )
            {
                LeafBTreeNode leaf = (LeafBTreeNode)node;

                seekLeaf( leaf, resultList );
            }
            else
            {
                throw new IllegalStateException( "node reported type other than InternalNode or LeafNode" );
            }
        }

        protected abstract void seekLeaf( LeafBTreeNode leaf, List<TResult> resultList );

        protected abstract void seekInternal( InternalBTreeNode internal, List<TResult> resultList );
    }
}
