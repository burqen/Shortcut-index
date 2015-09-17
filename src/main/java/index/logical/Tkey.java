package index.logical;

public class TKey<PROP extends Comparable<PROP>> implements Comparable<TKey<PROP>>
{
    private long id;
    private PROP prop;

    public TKey( long id, PROP prop )
    {
        this.id = id;
        this.prop = prop;
    }

    public long getId()
    {
        return id;
    }

    public PROP getProp()
    {
        return prop;
    }

    @Override
    public int compareTo( TKey<PROP> o )
    {
        return id == o.id ? prop.compareTo( o.prop ) : Long.compare( id, o.id );
    }

    @Override
    public int hashCode() {
        return (int) id * 23 + prop.hashCode();
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
        return String.format( "(%d,%s)", id, prop );
    }
}
