package index.logical;

public class TKey<prop extends Comparable<prop>> implements Comparable<TKey<prop>>
{
    long id;
    prop prop;

    public TKey( long id, prop prop )
    {
        this.id = id;
        this.prop = prop;
    }

    @Override
    public int compareTo( TKey<prop> o )
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
}
