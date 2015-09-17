package index.logical;

public class ShortcutIndexDescription
{
    private final String description;

    public ShortcutIndexDescription( String description )
    {
        this.description = description;
    }

    public String getDescription()
    {
        return description;
    }

    @Override
    public int hashCode()
    {
        return description.hashCode();
    }

    @Override
    public boolean equals( Object obj ) {
        if ( !( obj instanceof ShortcutIndexDescription ) )
            return false;
        if ( obj == this )
            return true;

        ShortcutIndexDescription rhs = (ShortcutIndexDescription) obj;
        return description.equals( rhs.description );
    }

    @Override
    public String toString()
    {
        return description;
    }
}
