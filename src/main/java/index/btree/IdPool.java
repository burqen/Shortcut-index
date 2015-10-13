package index.btree;

import java.util.LinkedList;
import java.util.Queue;

public class IdPool implements IdProvider
{
    private Queue<Long> freedIds;
    private long currentId;

    public IdPool()
    {
        freedIds = new LinkedList<>();
    }

    public void returnId( long id )
    {
        freedIds.offer( id );
    }

    @Override
    public long acquireNewId()
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
}
