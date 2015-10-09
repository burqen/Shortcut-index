package index.btree;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Used to search for keys in internal and leaf node.
 */
public class IndexSearch
{
    public static final int NO_POS = -1;

    /**
     * Search for keyAtPos such that key <= keyAtPos. Return first position of keyAtPos (not offset),
     * or NO_POS if no such key exist.
     *
     * On insert, key should be inserted at pos.
     * On seek in internal, child at pos should be followed from internal node, or last child if pos is NO_POS.
     * On seek in leaf, value at pos is correct if keyAtPos is equal to key.
     *
     * Simple implementation, linear search.
     *
     * //TODO: Implement binary search
     *
     * @param cursor    {@link PageCursor} attached to page with node (internal or leaf does not matter)
     * @param key       long[] of length 2 where key[0] is id and key[1] is property value
     * @return          first position i for which Node.KEY_COMPARATOR.compare( key, Node.keyAt( i ) <= 0;
     */
    public static int search( PageCursor cursor, long[] key )
    {
        int pos = 0;
        int keyCount = Node.keyCount( cursor );
        while ( pos < keyCount && Node.KEY_COMPARATOR.compare( key, Node.keyAt( cursor, pos ) ) > 0 )
        {
            pos++;
        }
        return pos == keyCount ? NO_POS : pos;
    }
}
