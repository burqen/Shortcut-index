package index.legacy;

import index.SCIndexDescription;
import index.SCIndex;

import java.io.PrintStream;
import java.util.List;

public class LegacySCIndex implements SCIndex
{
    private final SCIndexDescription description;

    private BTreeNode root;

    public LegacySCIndex( int order, SCIndexDescription description )
    {

        this.description = description;
        root = new LeafBTreeNode( order );
    }

    @Override
    public SCIndexDescription getDescription()
    {
        return description;
    }

    @Override
    public void insert( long[] key, long[] value )
    {
        insert( new TKey( key[0], key[1] ), new TValue( value[0], value[1] ) );
    }

    public void insert( TKey key, TValue value )
    {
        root.insert( key.getId(), key.getProp(), value );

        // Will only enter loop when split has occurred in root
        while ( root.getParent() != null )
        {
            root = root.getParent();
        }
    }

    public void seek( BTSeeker seeker, List<TResult> list )
    {
        seeker.seek( root, list );
    }

    // Methods mainly for testing

    public long totalKeyCount()
    {
        return root.totalKeyCount();
    }

    public int height()
    {
        return root.height();
    }

    public void printTree( PrintStream out )
    {
        root.printTree( out );
    }
}
