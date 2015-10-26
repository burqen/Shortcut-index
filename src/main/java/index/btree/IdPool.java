package index.btree;

import index.IdProvider;

public class IdPool implements IdProvider
{
    private long lastUsedId;

    public IdPool()
    {
        this( 0 );
    }
    public IdPool( long lastUsedId )
    {
        this.lastUsedId = lastUsedId;
    }

    @Override
    public long acquireNewId()
    {
        return ++lastUsedId;
    }
}
