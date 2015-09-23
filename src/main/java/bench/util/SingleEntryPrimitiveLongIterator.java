package bench.util;

import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

public class SingleEntryPrimitiveLongIterator implements PrimitiveLongIterator
{
    private long value;
    private boolean exhausted;

    public SingleEntryPrimitiveLongIterator( long value )
    {
        this.value = value;
    }

    @Override
    public boolean hasNext()
    {
        return !exhausted;
    }

    @Override
    public long next()
    {
        if ( exhausted )
        {
            throw new NoSuchElementException();
        }
        exhausted = true;
        return value;
    }
}
