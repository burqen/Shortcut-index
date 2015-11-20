package index.btree;

import index.SCResultVisitor;
import index.Seeker;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

public class Scanner extends Seeker.CommonSeeker
{

    @Override
    protected void seekLeaf( PageCursor cursor, Node node, SCResultVisitor visitor ) throws IOException
    {
        while ( true )
        {
            int keyCount = node.keyCount( cursor );
            for ( int i = 0; i < keyCount; i++ )
            {
                node.keyAt( cursor, i, keyHolder );
                node.valueAt( cursor, i, valueHolder );
                visitor.visit( keyHolder[0], keyHolder[1], valueHolder[0], valueHolder[1] );
            }
            long rightSibling = node.rightSibling( cursor );
            if ( rightSibling == Node.NO_NODE_FLAG )
            {
                break;
            }
            cursor.next( rightSibling );
        }
    }

    @Override
    protected void seekInternal( PageCursor cursor, Node node, SCResultVisitor visitor ) throws IOException
    {
        cursor.next( node.childAt( cursor, 0 ) );
        seek( cursor, node, visitor );
    }
}
