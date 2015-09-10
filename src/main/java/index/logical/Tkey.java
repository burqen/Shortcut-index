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
}
