package index.btree;

import java.util.LinkedList;
import java.util.Queue;

public class IdPool
{
    private Queue<Long> freedIds;
    private long currentId;

    public IdPool()
    {
        freedIds = new LinkedList<>();
    }
    public long getId()
    {
        if ( freedIds.isEmpty() )
        {
            return currentId++;
        }
        else
        {
            return freedIds.poll();
        }
    }

    public void returnId( long id )
    {
        freedIds.offer( id );
    }
}
