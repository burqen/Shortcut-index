package index.legacy;

import index.SCIndexDescription;
import index.SCKey;
import index.SCResult;
import index.SCValue;

import java.io.PrintStream;
import java.util.List;

public class LegacyIndex
{
    private final SCIndexDescription description;

    private LegacyBTreeNode root;

    public LegacyIndex( int order, SCIndexDescription description )
    {

        this.description = description;
        root = new LegacyLeafBTreeNode( order );
    }

    public SCIndexDescription getDescription()
    {
        return description;
    }

    public void insert( SCKey key, SCValue value )
    {
        root.insert( key.getId(), key.getProp(), value );

        // Will only enter loop when split has occurred in root
        while ( root.getParent() != null )
        {
            root = root.getParent();
        }
    }

    public void seek( LegacyBTSeeker seeker, List<SCResult> list )
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
