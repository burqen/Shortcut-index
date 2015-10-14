package index.legacy;

import index.SCIndexDescription;
import index.SCKey;
import index.SCResult;
import index.SCValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class LegacyRangeSeekerTest
{
    LegacyRangeSeeker rangeSeeker;
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
        rangeSeeker = new LegacyRangeSeeker( 1, 1l, 3l );
    }

    @Test
    public void oneLeafAllHitsWholeRange()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        rangeSeeker.seek( leaf, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 1 ).getValue() );
    }

    @Test
    public void oneLeafSomeHitsWholeRange()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        addEntryToLeaf( leaf, 1, 3 );
        rangeSeeker.seek( leaf, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 1 ).getValue() );
    }

    @Test
    public void oneLeafNoHitsWholeRange()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
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
        LegacyLeafBTreeNode left = new LegacyLeafBTreeNode( 2 );
        LegacyLeafBTreeNode right = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( left, 1, 0 );
        addEntryToLeaf( left, 1, 1 ); // Hit
        addEntryToLeaf( right, 1, 2 ); // Hit
        addEntryToLeaf( right, 1, 3 );
        addEntryToLeaf( right, 1, 4 );

        left.setRightSibling( right );

        rangeSeeker.seek( left, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 1 ).getValue() );
    }

    @Test
    public void seekFromInternalWholeRange()
    {
        LegacyInternalBTreeNode root = new LegacyInternalBTreeNode( 2 );

        root.setKey( 0, 1, 2 );
        LegacyLeafBTreeNode left = new LegacyLeafBTreeNode( 2 );
        LegacyLeafBTreeNode right = new LegacyLeafBTreeNode( 2 );

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
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 1 ).getValue() );
    }

    // DEFINED FROM
    LegacyRangeSeeker fromSeeker;

    @Before
    public void setupFromSeeker()
    {
        fromSeeker = new LegacyRangeSeeker( 1, 1l, null );
    }

    @Test
    public void oneLeafAllHitsDefineFrom()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        fromSeeker.seek( leaf, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 1 ).getValue() );
    }

    @Test
    public void oneLeafSomeHitsDefineFrom()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        addEntryToLeaf( leaf, 1, 3 );
        fromSeeker.seek( leaf, list );

        assertEquals( "Expected to find 3 hits", 3, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 3, 3 ), list.get( 2 ).getValue() );
    }

    @Test
    public void oneLeafNoHitsDefineFrom()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
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
        LegacyLeafBTreeNode left = new LegacyLeafBTreeNode( 2 );
        LegacyLeafBTreeNode right = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( left, 1, 0 );
        addEntryToLeaf( left, 1, 1 ); // Hit
        addEntryToLeaf( right, 1, 2 ); // Hit
        addEntryToLeaf( right, 1, 3 ); // Hit
        addEntryToLeaf( right, 1, 4 ); // Hit

        left.setRightSibling( right );

        fromSeeker.seek( left, list );

        assertEquals( "Expected to find 4 hits", 4, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 3, 3 ), list.get( 2 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 4, 4 ), list.get( 3 ).getValue() );
    }

    @Test
    public void seekFromInternalDefineFrom()
    {
        LegacyInternalBTreeNode root = new LegacyInternalBTreeNode( 2 );

        root.setKey( 0, 1, 2 );
        LegacyLeafBTreeNode left = new LegacyLeafBTreeNode( 2 );
        LegacyLeafBTreeNode right = new LegacyLeafBTreeNode( 2 );

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
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 3, 3 ), list.get( 2 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 4, 4 ), list.get( 3 ).getValue() );
    }


    // DEFINED TO
    LegacyRangeSeeker toSeeker;

    @Before
    public void setupToSeeker()
    {
        toSeeker = new LegacyRangeSeeker( 1, null, 3l );
    }

    @Test
    public void oneLeafAllHitsDefineTo()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        toSeeker.seek( leaf, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 1 ).getValue() );
    }

    @Test
    public void oneLeafSomeHitsDefineTo()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        addEntryToLeaf( leaf, 1, 3 );
        toSeeker.seek( leaf, list );

        assertEquals( "Expected to find 3 hits", 3, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 0, 0 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 2 ).getValue() );
    }

    @Test
    public void oneLeafNoHitsDefineTo()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 0, 1 );
        addEntryToLeaf( leaf, 0, 2 );
        addEntryToLeaf( leaf, 0, 3 );
        toSeeker.seek( leaf, list );

        assertEquals( "Expected to find 1 hits", 1, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 0, 0 ), list.get( 0 ).getValue() );
    }

    @Test
    public void multipleLeafSomeHitsDefineTo()
    {
        LegacyLeafBTreeNode left = new LegacyLeafBTreeNode( 2 );
        LegacyLeafBTreeNode right = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( left, 1, 0 ); // Hit
        addEntryToLeaf( left, 1, 1 ); // Hit
        addEntryToLeaf( right, 1, 2 ); // Hit
        addEntryToLeaf( right, 1, 3 );
        addEntryToLeaf( right, 1, 4 );

        left.setRightSibling( right );

        toSeeker.seek( left, list );

        assertEquals( "Expected to find 3 hits", 3, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 0, 0 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 2 ).getValue() );
    }

    @Test
    public void seekFromInternalDefineTo()
    {
        LegacyInternalBTreeNode root = new LegacyInternalBTreeNode( 2 );

        root.setKey( 0, 1, 2 );
        LegacyLeafBTreeNode left = new LegacyLeafBTreeNode( 2 );
        LegacyLeafBTreeNode right = new LegacyLeafBTreeNode( 2 );

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
        assertEquals( "Expected results to be correctly ordered", new SCValue( 0, 0 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 2 ).getValue() );
    }

    // No property restriction
    LegacyRangeSeeker allSeeker;

    @Before
    public void setupAllSeeker()
    {
        allSeeker = new LegacyRangeSeeker( 1, null, null );
    }

    @Test
    public void oneLeafAllHitsDefineNone()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        allSeeker.seek( leaf, list );

        assertEquals( "Expected to find 2 hits", 2, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 1 ).getValue() );
    }

    @Test
    public void oneLeafSomeHitsDefineNone()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 1, 1 );
        addEntryToLeaf( leaf, 1, 2 );
        addEntryToLeaf( leaf, 1, 3 );
        allSeeker.seek( leaf, list );

        assertEquals( "Expected to find 4 hits", 4, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 0, 0 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 2 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 3, 3 ), list.get( 3 ).getValue() );
    }

    @Test
    public void oneLeafNoHitsDefineNone()
    {
        LegacyLeafBTreeNode leaf = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( leaf, 1, 0 );
        addEntryToLeaf( leaf, 0, 1 );
        addEntryToLeaf( leaf, 0, 2 );
        addEntryToLeaf( leaf, 0, 3 );
        allSeeker.seek( leaf, list );

        assertEquals( "Expected to find 1 hits", 1, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 0, 0 ), list.get( 0 ).getValue() );
    }

    @Test
    public void multipleLeafSomeHitsDefineNone()
    {
        LegacyLeafBTreeNode left = new LegacyLeafBTreeNode( 2 );
        LegacyLeafBTreeNode right = new LegacyLeafBTreeNode( 2 );
        addEntryToLeaf( left, 1, 0 ); // Hit
        addEntryToLeaf( left, 1, 1 ); // Hit
        addEntryToLeaf( right, 1, 2 ); // Hit
        addEntryToLeaf( right, 1, 3 );
        addEntryToLeaf( right, 1, 4 );

        left.setRightSibling( right );

        allSeeker.seek( left, list );

        assertEquals( "Expected to find 5 hits", 5, list.size() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 0, 0 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 2 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 3, 3 ), list.get( 3 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 4, 4 ), list.get( 4 ).getValue() );
    }

    @Test
    public void seekFromInternalDefineNone()
    {
        LegacyInternalBTreeNode root = new LegacyInternalBTreeNode( 2 );

        root.setKey( 0, 1, 2 );
        LegacyLeafBTreeNode left = new LegacyLeafBTreeNode( 2 );
        LegacyLeafBTreeNode right = new LegacyLeafBTreeNode( 2 );

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
        assertEquals( "Expected results to be correctly ordered", new SCValue( 0, 0 ), list.get( 0 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 1, 1 ), list.get( 1 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 2, 2 ), list.get( 2 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 3, 3 ), list.get( 3 ).getValue() );
        assertEquals( "Expected results to be correctly ordered", new SCValue( 4, 4 ), list.get( 4 ).getValue() );
    }

    // Whole range in index

    @Test
    public void seekRootIsLeaf()
    {
        SCKey startPoint = new SCKey( 1, 1 );
        SCValue startValue = new SCValue( 1, 1 );
        index.insert( startPoint, startValue );

        rangeSeeker = new LegacyRangeSeeker( 1l, 1l, null );
        index.seek( rangeSeeker, list );

        assertEquals( "Expected exactly one result", 1, list.size() );
        assertEquals( "", new SCResult( startPoint, startValue ), list.get( 0 ) );
    }

    @Test
    public void seekRangeUniqueKeys()
    {
        for ( int i = 0; i < 100; i++ )
        {
            index.insert( new SCKey( 1, i ), new SCValue( i, i ) );
        }

        for ( int i = 0; i < 100; i++ )
        {
            list = new ArrayList<>();
            rangeSeeker = new LegacyRangeSeeker( 1l, 0l, (long)i );
            index.seek( rangeSeeker, list );
            assertEquals( "Expected to find " + i + " results.", i, list.size() );
            for ( int j = 0; j < i; j++ )
            {
                assertEquals( "Expected results to match",
                        new SCResult( new SCKey( 1, j ), new SCValue( j, j ) ), list.get( j ) );
            }
        }
    }
}
