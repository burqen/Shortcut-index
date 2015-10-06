package index.logical;

import org.junit.Assert;
import org.junit.Test;

public class BTreeNodeTest
{
    @Test
    public void split()
    {
        int order = 4;
        Integer[] left = new Integer[]{1,2,3,4,5,6,7,8};
        Integer[] right = new Integer[order*2];
        Integer overflow = 9;
        BTreeNode.split( left, right, overflow );
        Assert.assertArrayEquals( "Left is not split correctly", new Integer[]{1, 2, 3, 4, null, null, null, null},
                left );
        Assert.assertArrayEquals( "Right is not split correctly", new Integer[]{5,6,7,8,9,null,null,null}, right );
    }

    @Test
    public void splitKeys()
    {
        int order = 4;
        long[] left = new long[]{1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8};
        long [] right = new long[order*2*BTreeNode.KEY_SIZE];
        TKey overflow = new TKey( 9, 9 );

        BTreeNode.splitKeys( left, right, overflow );
        long[] leftAfterSplit = new long[4*2];
        long[] rightAfterSplit = new long[5*2];
        System.arraycopy( left, 0, leftAfterSplit, 0, 4*2 );
        System.arraycopy( right, 0, rightAfterSplit, 0, 5*2 );
        Assert.assertArrayEquals( "Left is not split correctly", new long[]{1,1,2,2,3,3,4,4},
                leftAfterSplit );
        Assert.assertArrayEquals( "Right is not split correctly", new long[]{5,5,6,6,7,7,8,8,9,9},
                rightAfterSplit );
    }

    @Test
    public void replaceKey()
    {

    }
}
