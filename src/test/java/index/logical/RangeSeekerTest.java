package index.logical;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class RangeSeekerTest
{
    RangeSeeker rangeSeeker;
    int order = 2;
    ShortcutIndexService index;
    List<TResult> list;

    @Before
    public void setup()
    {
        index = new ShortcutIndexService( order );
        list = new ArrayList<>();
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
        rangeSeeker = new RangeSeeker( 1, 1l, 3l );
    }

    @Test
    public void oneLeafAllHitsWholeRange()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        rangeSeeker.seek( leaf, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 1 ).getValue() );
    }

    @Test
    public void oneLeafSomeHitsWholeRange()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        addEntryToLeaf( leaf, 1, 3 );
        rangeSeeker.seek( leaf, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 1 ).getValue() );
    }

    @Test
    public void oneLeafNoHitsWholeRange()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 0, 1 );
        addEntryToLeaf( leaf, 0, 2 );
        addEntryToLeaf( leaf, 1, 3 );
        rangeSeeker.seek( leaf, list );

        assertEquals( "Expected to find 0 hits", 0, list.size() );
    }

    @Test
    public void multipleLeafSomeHitsWholeRange()
    {
        LeafBTreeNode left = new LeafBTreeNode( 2 );
        LeafBTreeNode right = new LeafBTreeNode( 2 );
        addEntryToLeaf( left, 1, 0 );
        addEntryToLeaf( left, 1, 1 ); // Hit
        addEntryToLeaf( right, 1, 2 ); // Hit
        addEntryToLeaf( right, 1, 3 );
        addEntryToLeaf( right, 1, 4 );

        left.setRightSibling( right );

        rangeSeeker.seek( left, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 1 ).getValue() );
    }

    @Test
    public void seekFromInternalWholeRange()
    {
        InternalBTreeNode root = new InternalBTreeNode( 2 );

        root.setKey( 0, new TKey( 1,2 ) );
        LeafBTreeNode left = new LeafBTreeNode( 2 );
        LeafBTreeNode right = new LeafBTreeNode( 2 );

        addEntryToLeaf( left, 1, 0 );
        addEntryToLeaf( left, 1, 1 );
        addEntryToLeaf( right, 1, 2 );
        addEntryToLeaf( right, 1, 3 );
        addEntryToLeaf( right, 1, 4 );

        root.setChild( 0, left );
        root.setChild( 1, right );
        left.setRightSibling( right );

        rangeSeeker.seek( root, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 1 ).getValue() );
    }

    // DEFINED FROM
    RangeSeeker fromSeeker;

    @Before
    public void setupFromSeeker()
    {
        fromSeeker = new RangeSeeker( 1, 1l, null );
    }

    @Test
    public void oneLeafAllHitsDefineFrom()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        fromSeeker.seek( leaf, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 1 ).getValue() );
    }

    @Test
    public void oneLeafSomeHitsDefineFrom()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        addEntryToLeaf( leaf, 1, 3 );
        fromSeeker.seek( leaf, list );

        assertEquals( "Expected to find 3 hits", 3, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 3, 3 ), list.get( 2 ).getValue() );
    }

    @Test
    public void oneLeafNoHitsDefineFrom()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 0, 1 );
        addEntryToLeaf( leaf, 0, 2 );
        addEntryToLeaf( leaf, 0, 3 );
        fromSeeker.seek( leaf, list );

        assertEquals( "Expected to find 0 hits", 0, list.size() );
    }

    @Test
    public void multipleLeafSomeHitsDefineFrom()
    {
        LeafBTreeNode left = new LeafBTreeNode( 2 );
        LeafBTreeNode right = new LeafBTreeNode( 2 );
        addEntryToLeaf( left, 1, 0 );
        addEntryToLeaf( left, 1, 1 ); // Hit
        addEntryToLeaf( right, 1, 2 ); // Hit
        addEntryToLeaf( right, 1, 3 ); // Hit
        addEntryToLeaf( right, 1, 4 ); // Hit

        left.setRightSibling( right );

        fromSeeker.seek( left, list );

        assertEquals( "Expected to find 4 hits", 4, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 3, 3 ), list.get( 2 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 4, 4 ), list.get( 3 ).getValue() );
    }

    @Test
    public void seekFromInternalDefineFrom()
    {
        InternalBTreeNode root = new InternalBTreeNode( 2 );

        root.setKey( 0, new TKey( 1, 2 ) );
        LeafBTreeNode left = new LeafBTreeNode( 2 );
        LeafBTreeNode right = new LeafBTreeNode( 2 );

        addEntryToLeaf( left, 1, 0 );
        addEntryToLeaf( left, 1, 1 );
        addEntryToLeaf( right, 1, 2 );
        addEntryToLeaf( right, 1, 3 );
        addEntryToLeaf( right, 1, 4 );

        root.setChild( 0, left );
        root.setChild( 1, right );
        left.setRightSibling( right );

        fromSeeker.seek( root, list );

        assertEquals( "Expected to find 4 hits", 4, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 3, 3 ), list.get( 2 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 4, 4 ), list.get( 3 ).getValue() );
    }


    // DEFINED TO
    RangeSeeker toSeeker;

    @Before
    public void setupToSeeker()
    {
        toSeeker = new RangeSeeker( 1, null, 3l );
    }

    @Test
    public void oneLeafAllHitsDefineTo()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        toSeeker.seek( leaf, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 1 ).getValue() );
    }

    @Test
    public void oneLeafSomeHitsDefineTo()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        addEntryToLeaf( leaf, 1, 3 );
        toSeeker.seek( leaf, list );

        assertEquals( "Expected to find 3 hits", 3, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 0, 0 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 2 ).getValue() );
    }

    @Test
    public void oneLeafNoHitsDefineTo()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 0, 1 );
        addEntryToLeaf( leaf, 0, 2 );
        addEntryToLeaf( leaf, 0, 3 );
        toSeeker.seek( leaf, list );

        assertEquals( "Expected to find 1 hits", 1, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 0, 0 ), list.get( 0 ).getValue() );
    }

    @Test
    public void multipleLeafSomeHitsDefineTo()
    {
        LeafBTreeNode left = new LeafBTreeNode( 2 );
        LeafBTreeNode right = new LeafBTreeNode( 2 );
        addEntryToLeaf( left, 1, 0 ); // Hit
        addEntryToLeaf( left, 1, 1 ); // Hit
        addEntryToLeaf( right, 1, 2 ); // Hit
        addEntryToLeaf( right, 1, 3 );
        addEntryToLeaf( right, 1, 4 );

        left.setRightSibling( right );

        toSeeker.seek( left, list );

        assertEquals( "Expected to find 3 hits", 3, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 0, 0 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 2 ).getValue() );
    }

    @Test
    public void seekFromInternalDefineTo()
    {
        InternalBTreeNode root = new InternalBTreeNode( 2 );

        root.setKey( 0, new TKey( 1,2 ) );
        LeafBTreeNode left = new LeafBTreeNode( 2 );
        LeafBTreeNode right = new LeafBTreeNode( 2 );

        addEntryToLeaf( left, 1, 0 );
        addEntryToLeaf( left, 1, 1 );
        addEntryToLeaf( right, 1, 2 );
        addEntryToLeaf( right, 1, 3 );
        addEntryToLeaf( right, 1, 4 );

        root.setChild( 0, left );
        root.setChild( 1, right );
        left.setRightSibling( right );

        toSeeker.seek( root, list );

        assertEquals( "Expected to find 3 hits", 3, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 0, 0 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 2 ).getValue() );
    }

    // No property restriction
    RangeSeeker allSeeker;

    @Before
    public void setupAllSeeker()
    {
        allSeeker = new RangeSeeker( 1, null, null );
    }

    @Test
    public void oneLeafAllHitsDefineNone()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        allSeeker.seek( leaf, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 1 ).getValue() );
    }

    @Test
    public void oneLeafSomeHitsDefineNone()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        addEntryToLeaf( leaf, 1, 3 );
        allSeeker.seek( leaf, list );

        assertEquals( "Expected to find 4 hits", 4, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 0, 0 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 2 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 3, 3 ), list.get( 3 ).getValue() );
    }

    @Test
    public void oneLeafNoHitsDefineNone()
    {
        LeafBTreeNode leaf = new LeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 0, 1 );
        addEntryToLeaf( leaf, 0, 2 );
        addEntryToLeaf( leaf, 0, 3 );
        allSeeker.seek( leaf, list );

        assertEquals( "Expected to find 1 hits", 1, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 0, 0 ), list.get( 0 ).getValue() );
    }

    @Test
    public void multipleLeafSomeHitsDefineNone()
    {
        LeafBTreeNode left = new LeafBTreeNode( 2 );
        LeafBTreeNode right = new LeafBTreeNode( 2 );
        addEntryToLeaf( left, 1, 0 ); // Hit
        addEntryToLeaf( left, 1, 1 ); // Hit
        addEntryToLeaf( right, 1, 2 ); // Hit
        addEntryToLeaf( right, 1, 3 );
        addEntryToLeaf( right, 1, 4 );

        left.setRightSibling( right );

        allSeeker.seek( left, list );

        assertEquals( "Expected to find 5 hits", 5, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 0, 0 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 2 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 3, 3 ), list.get( 3 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 4, 4 ), list.get( 4 ).getValue() );
    }

    @Test
    public void seekFromInternalDefineNone()
    {
        InternalBTreeNode root = new InternalBTreeNode( 2 );

        root.setKey( 0, new TKey( 1,2 ) );
        LeafBTreeNode left = new LeafBTreeNode( 2 );
        LeafBTreeNode right = new LeafBTreeNode( 2 );

        addEntryToLeaf( left, 1, 0 );
        addEntryToLeaf( left, 1, 1 );
        addEntryToLeaf( right, 1, 2 );
        addEntryToLeaf( right, 1, 3 );
        addEntryToLeaf( right, 1, 4 );

        root.setChild( 0, left );
        root.setChild( 1, right );
        left.setRightSibling( right );

        allSeeker.seek( root, list );

        assertEquals( "Expected to find 5 hits", 5, list.size() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 0, 0 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 1, 1 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 2, 2 ), list.get( 2 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 3, 3 ), list.get( 3 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new TValue( 4, 4 ), list.get( 4 ).getValue() );
    }

    // Whole range in index

    @Test
    public void seekRootIsLeaf()
    {
        TKey startPoint = new TKey( 1, 1 );
        TValue startValue = new TValue( 1, 1 );
        index.insert( startPoint, startValue );

        rangeSeeker = new RangeSeeker( 1l, 1l, null );
        index.seek( rangeSeeker, list );

        assertEquals( "Expected exactly one result", 1, list.size() );
        assertEquals( "", new TResult( startPoint, startValue ), list.get( 0 ) );
    }

    @Test
    public void seekRangeUniqueKeys()
    {
        for ( int i = 0; i < 100; i++ )
        {
            index.insert( new TKey( 1, i ), new TValue( i, i ) );
        }

        for ( int i = 0; i < 100; i++ )
        {
            list = new ArrayList<>();
            rangeSeeker = new RangeSeeker( 1l, 0l, (long)i );
            index.seek( rangeSeeker, list );
            assertEquals( "Expected to find " + i + " results.", i, list.size() );
            for ( int j = 0; j < i; j++ )
            {
                assertEquals( "Expected results to match",
                        new TResult( new TKey( 1, j ), new TValue( j, j ) ), list.get( j ) );
            }
        }
    }
}
