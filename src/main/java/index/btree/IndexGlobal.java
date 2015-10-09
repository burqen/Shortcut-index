package index.btree;

import static index.btree.Node.HEADER_LENGTH;
import static index.btree.Node.SIZE_CHILD;
import static index.btree.Node.SIZE_KEY;
import static index.btree.Node.SIZE_VALUE;


public class IndexGlobal
{
    public static final int PAGE_SIZE = 8192;


    public static final int MAX_KEY_COUNT_INTERNAL = Math.floorDiv( PAGE_SIZE - (HEADER_LENGTH + SIZE_CHILD),
            SIZE_KEY + SIZE_CHILD);
    public static final int MAX_KEY_COUNT_LEAF = Math.floorDiv( PAGE_SIZE - HEADER_LENGTH,
            SIZE_KEY + SIZE_VALUE );

}
