package index.legacy;

import java.util.List;

public class RangeSeeker extends BTSeeker.CommonSeeker
{
    private final long id;
    private final Long from;
    private final Long to;

    /**
     * Searching for keys with id and prop in range [from,to). Including from, excluding to.
     * from <= value < to
     * @param id    id that key should match
     * @param from  from in range or null if range has no min value
     * @param to    to in range or null if range has no to value
     */
    public RangeSeeker( long id, Long from, Long to )
    {
        this.id = id;
        this.from = from;
        this.to = to;
    }

    @Override
    protected void seekInternal( InternalBTreeNode internal, List<TResult> resultList )
    {
        BTreeNode child = null;

        for ( int i = 0; i < internal.getKeyCount(); i++ )
        {
            TKey key = internal.getKey( i );
            if ( key.getId() > id || ( key.getId() == id && nullSafeCompare( key.getProp(), from, false ) > 0 ) )
            {
                child = internal.getChild( i );
                break;
            }
        }

        if ( child == null )
        {
            // No key larger than (id,from) found.
            // Continue search in child containing largest keys.
            child = internal.getChild( internal.getKeyCount() );
        }

        seek( child, resultList );
    }

    @Override
    protected void seekLeaf( LeafBTreeNode leaf, List<TResult> resultList )
    {
        for ( int i = 0; i < leaf.getKeyCount(); i++ )
        {
            TKey key = leaf.getKey( i );
            long keyId = key.getId();
            long prop = key.getProp();

            if ( keyId == id && nullSafeCompare( prop, from, false ) >= 0 )
            {
                if ( nullSafeCompare( prop, to, true ) < 0 )
                {
                    resultList.add( new TResult( key, leaf.getValue( i ) ) );
                }
                else
                {
                    return;
                }
            }
            else if ( keyId > id || ( keyId == id && nullSafeCompare( prop, to, true ) >= 0 ) )
            {
                return;
            }
        }

        LeafBTreeNode rightSibling = (LeafBTreeNode)leaf.getRightSibling();
        if ( rightSibling != null )
        {
            seekLeaf( rightSibling, resultList );
        }
    }

    public static int nullSafeCompare( Long first, Long other, boolean nullIsHigh )
    {
        if ( first == null && other == null)
        {
            return 0;
        }
        if ( first == null ^ other == null )
        {
            if ( nullIsHigh )
            {
                return (first == null) ? 1 : -1;
            }
            else
            {
                return (first == null) ? -1 : 1;
            }
        }

        return first.compareTo( other );
    }
}
