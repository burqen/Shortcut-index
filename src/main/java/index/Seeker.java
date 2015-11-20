package index;

import index.btree.Node;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

public interface Seeker
{
    /**
     * Cursor will be moved from page.
     * @param cursor        {@link org.neo4j.io.pagecache.PageCursor} pinned to page with node (internal or leaf)
     * @param visitor    {@link java.util.List} where found results will be stored
     * @throws IOException  on cursor failure
     */
    void seek( PageCursor cursor, Node node, SCResultVisitor visitor ) throws IOException;

    public abstract class CommonSeeker implements Seeker
    {

        // TODO: A lot of time is spent in the seek method, both for seek and scan. Can we make it faster?
        // TODO: Maybe with binary search in IndexSearch.
        public void seek( PageCursor cursor, Node node, SCResultVisitor visitor ) throws IOException
        {
            if ( node.isInternal( cursor ) )
            {
                seekInternal( cursor, node, visitor );
            }
            else if ( node.isLeaf( cursor ) )
            {
                seekLeaf( cursor, node, visitor );
            }
            else
            {
                throw new IllegalStateException( "node reported type other than internal or leaf" );
            }
        }

        protected abstract void seekLeaf( PageCursor cursor, Node node, SCResultVisitor visitor ) throws IOException;

        protected abstract void seekInternal( PageCursor cursor, Node node, SCResultVisitor visitor ) throws IOException;
    }
}
