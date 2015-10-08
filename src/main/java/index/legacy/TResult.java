package index.legacy;

public class TResult
{
    private final TKey key;
    private final TValue value;

    public TResult( TKey key, TValue value )
    {
        this.key = key;
        this.value = value;
    }

    public TKey getKey()
    {
        return key;
    }

    public TValue getValue()
    {
        return value;
    }

    @Override
    public int hashCode()
    {
        return key.hashCode() * 23 + value.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( !( obj instanceof TResult ) )
            return false;
        if ( obj == this )
            return true;

        TResult rhs = (TResult) obj;
        return this.getKey().equals( rhs.getKey() ) && getValue().equals( rhs.getValue() );
    }
}
