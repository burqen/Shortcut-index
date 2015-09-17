package index.logical;

import java.util.Objects;

public class TKey implements Comparable<TKey>
{
    private long id;
    private long prop;

    public TKey( long id, long prop )
    {
        this.id = id;
        this.prop = prop;
    }

    public long getId()
    {
        return id;
    }

    public long getProp()
    {
        return prop;
    }

    @Override
    public int compareTo( TKey o )
    {
        Objects.requireNonNull( o );
        return id == o.id ? Long.compare( prop, o.prop ) : Long.compare( id, o.id );
    }

    @Override
    public int hashCode() {
        return (int) ( id * 23 + prop );
    }

    @Override
    public boolean equals( Object obj ) {
        if ( !( obj instanceof TKey ) )
            return false;
        if ( obj == this )
            return true;

        TKey rhs = (TKey) obj;
        return this.compareTo( rhs ) == 0;
    }

    @Override
    public String toString()
    {
        return String.format( "(%d,%d)", id, prop );
    }

}
