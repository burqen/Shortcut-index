package index.logical;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class ExactMatchTest
{
    ExactMatchSeeker matchSeeker;
    int order = 2;
    ShortcutIndexService index;
    List<TResult> list;

    @Before
    public void setup()
    {
        index = new ShortcutIndexService( order );
        list = new ArrayList();
    }

    // Utility
    private void addEntryToLeaf( LeafBTreeNode leaf, long id, long prop )
    {
        leaf.insert( new TKey( id, prop ), new TValue( prop, prop ) );
    }

    // DEFINED WHOLE RANGE


    @Before
    public void setupRangeSeeker()
    {
        matchSeeker = new ExactMatchSeeker( new TKey( 1, 1l ) );
    }

    @Test
    public void oneLeafWithOneHit()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        matchSeeker.seek( leaf, list );

        assertEquals( "Expected to find 1 hits", 1, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 0 ).getValue() );
    }

    @Test
    public void oneLeafWithMultipleHits()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 3 );
        matchSeeker.seek( leaf, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 1 ).getValue() );
    }

    @Test
    public void oneLeafWithNoHits()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 0, 1 );
        addEntryToLeaf( leaf, 0, 2 );
        addEntryToLeaf( leaf, 1, 3 );
        matchSeeker.seek( leaf, list );

        assertEquals( "Expected to find 0 hits", 0, list.size() );
    }

    @Test
    public void multipleLeafMultipleHits()
    {
        LeafBTreeNode left = new LeafBTreeNode( 2 );
        LeafBTreeNode right = new LeafBTreeNode( 2 );
        addEntryToLeaf( left, 1, 0 );
        addEntryToLeaf( left, 1, 1 ); // Hit
        addEntryToLeaf( right, 1, 1 ); // Hit
        addEntryToLeaf( right, 1, 3 );
        addEntryToLeaf( right, 1, 4 );

        left.setRightSibling( right );

        matchSeeker.seek( left, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 1 ).getValue() );
    }

    @Test
    public void seekFromInternal()
    {
        InternalBTreeNode root = new InternalBTreeNode( 2 );

        root.setKey( 0, new TKey( 1,2 ) );
        LeafBTreeNode left = new LeafBTreeNode( 2 );
        LeafBTreeNode right = new LeafBTreeNode( 2 );

        addEntryToLeaf( left, 1, 0 );
        addEntryToLeaf( left, 1, 1 );
        addEntryToLeaf( right, 1, 1 );
        addEntryToLeaf( right, 1, 3 );
        addEntryToLeaf( right, 1, 4 );

        root.setChild( 0, left );
        root.setChild( 1, right );
        left.setRightSibling( right );

        matchSeeker.seek( root, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 1 ).getValue() );
    }
}