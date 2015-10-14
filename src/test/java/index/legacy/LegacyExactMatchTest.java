package index.legacy;

import index.SCIndexDescription;
import index.SCIndex;
import index.SCKey;
import index.SCResult;
import index.SCValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class LegacyExactMatchTest
{
    LegacyExactMatchSeeker matchSeeker;
    int order = 2;
    LegacyIndex index;
    List<SCResult> list;
    @Mock
    SCIndexDescription desc;

    @Before
    public void setup()
    {
        index = new LegacyIndex( order, desc );
        list = new ArrayList<>();
    }

    // Utility
    private void addEntryToLeaf( LegacyLeafBTreeNode leaf, long id, long prop )
    {
        leaf.insert( id, prop, new SCValue( prop, prop ) );
    }

    // DEFINED WHOLE RANGE


    @Before
    public void setupRangeSeeker()
    {
        matchSeeker = new LegacyExactMatchSeeker( new SCKey( 1, 1l ) );
    }

    @Test
    public void oneLeafWithOneHit()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        matchSeeker.seek( leaf, list );

        assertEquals( "Expected to find 1 hits", 1, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 0 ).getValue() );
    }

    @Test
    public void oneLeafWithMultipleHits()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 3 );
        matchSeeker.seek( leaf, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 1 ).getValue() );
    }

    @Test
    public void oneLeafWithNoHits()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
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
        LegacyLeafBTreeNode left = new LegacyLeafBTreeNode( 2 );
        LegacyLeafBTreeNode right = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( left, 1, 0 );
        addEntryToLeaf( left, 1, 1 ); // Hit
        addEntryToLeaf( right, 1, 1 ); // Hit
        addEntryToLeaf( right, 1, 3 );
        addEntryToLeaf( right, 1, 4 );

        left.setRightSibling( right );

        matchSeeker.seek( left, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 1 ).getValue() );
    }

    @Test
    public void seekFromInternal()
    {
        LegacyInternalBTreeNode root = new LegacyInternalBTreeNode( 2 );

        root.setKey( 0, 1, 2 );
        LegacyLeafBTreeNode left = new LegacyLeafBTreeNode( 2 );
        LegacyLeafBTreeNode right = new LegacyLeafBTreeNode( 2 );

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
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 1 ).getValue() );
    }
}
