package index.btree;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Implementation of the insert algorithm in this B+ tree including split.
 * Takes storage format into consideration.
 */
public class IndexInsert
{

    public static SplitResult insert( PageCursor cursor, long[] key, long[] value)
    {
        SplitResult split;

        if ( Node.isLeaf( cursor ) )
        {
            split = insertInLeaf( cursor, key, value );
        }
        else
        {
            split = insertInInternal( cursor, key, value );
        }

        return split;
    }

    private static SplitResult insertInInternal( PageCursor cursor, long[] key, long[] value )
    {
        throw new NotImplementedException();
    }

    private static SplitResult insertInLeaf( PageCursor cursor, long[] key, long[] value )
    {
        int keyCount = Node.keyCount( cursor );

        if ( keyCount < IndexGlobal.MAX_KEY_COUNT_LEAF )
        {
            int pos = IndexSearch.search( cursor, key );

        }
        else
        {

        }


        return null;
    }
}
