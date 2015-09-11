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
}
