package index.btree;

import static index.btree.Node.HEADER_LENGTH;
import static index.btree.Node.SIZE_CHILD;
import static index.btree.Node.SIZE_KEY;
import static index.btree.Node.SIZE_VALUE;

/**
 * Using Separate design the internal nodes should look like
 *
 * # = empty space
 *
 * [                  HEADER            ]|[      KEYS     ]|[     CHILDREN      ]
 * [TYPE][KEYCOUNT][PARENT][RIGHTSIBLING]|[[KEY][KEY]...##]|[[CHILD][CHILD]...##]
 *  0     1         5       13             21
 *
 * Calc offset for key i (starting from 0)
 * HEADER_LENGTH + i * SIZE_KEY
 *
 * Calc offset for child i
 * HEADER_LENGTH + SIZE_KEY * MAX_KEY_COUNT_INTERNAL + i * SIZE_CHILD
 *
 *
 * Using Separate design the leaf nodes should look like
 *
 *
 * [                  HEADER            ]|[      KEYS     ]|[       VALUES      ]
 * [TYPE][KEYCOUNT][PARENT][RIGHTSIBLING]|[[KEY][KEY]...##]|[[VALUE][VALUE]...##]
 *  0     1         5       13             21
 *
 * Calc offset for key i (starting from 0)
 * HEADER_LENGTH + i * SIZE_KEY
 *
 * Calc offset for value i
 * HEADER_LENGTH + SIZE_KEY * MAX_KEY_COUNT_LEAF + i * SIZE_VALUE
 *
 */
public class IndexGlobal
{
    public static final int PAGE_SIZE = 8192;


    public static final int MAX_KEY_COUNT_INTERNAL = Math.floorDiv( PAGE_SIZE - (HEADER_LENGTH + SIZE_CHILD),
            SIZE_KEY + SIZE_CHILD);
    public static final int MAX_KEY_COUNT_LEAF = Math.floorDiv( PAGE_SIZE - HEADER_LENGTH,
            SIZE_KEY + SIZE_VALUE );

}
