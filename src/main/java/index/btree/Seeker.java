package index.btree;

import index.legacy.TResult;

import java.io.IOException;
import java.util.List;

import org.neo4j.io.pagecache.PageCursor;

public interface Seeker
{
    /**
     * Cursor will be moved from page.
     * @param cursor        {@link PageCursor} pinned to page with node (internal or leaf)
     * @param resultList    {@link java.util.List} where found results will be stored
     * @throws IOException  on cursor failure
     */
    void seek( PageCursor cursor, List<TResult> resultList ) throws IOException;

    public abstract class CommonSeeker implements Seeker
    {
        protected Node node;

        public CommonSeeker( Node node )
        {
            this.node = node;
        }

        public void seek( PageCursor cursor, List<TResult> resultList ) throws IOException
        {
            if ( node.isInternal( cursor ) )
            {
                seekInternal( cursor, resultList );
            }
            else if ( node.isLeaf( cursor ) )
            {
                seekLeaf( cursor, resultList );
            }
            else
            {
                throw new IllegalStateException( "node reported type other than internal or leaf" );
            }
        }

        protected abstract void seekLeaf( PageCursor cursor, List<TResult> resultList ) throws IOException;

        protected abstract void seekInternal( PageCursor cursor, List<TResult> resultList ) throws IOException;
    }
}
