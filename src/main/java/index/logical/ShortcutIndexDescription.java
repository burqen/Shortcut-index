package index.logical;

import org.neo4j.graphdb.Direction;

public class ShortcutIndexDescription
{
    private String description;
    public final String firstLabel;
    public final String secondLabel;
    public final String relationshipType;
    public final Direction direction;
    public final String relationshipPropertyKey;
    public final String nodePropertyKey;

    public ShortcutIndexDescription( )
    {
        this( "", "", "", Direction.BOTH, "", null );
    }

    public ShortcutIndexDescription( String firstLabel,
            String secondLabel,
            String relationshipType,
            Direction direction,
            String relationshipPropertyKey,
            String nodePropertyKey )
    {
        if ( relationshipPropertyKey == null ^ nodePropertyKey == null )
        {
            this.firstLabel = firstLabel.toUpperCase();
            this.secondLabel = secondLabel.toUpperCase();
            this.relationshipType = relationshipType.toUpperCase();
            this.direction = direction;
            this.relationshipPropertyKey =
                    relationshipPropertyKey == null ? null : relationshipPropertyKey.toLowerCase();
            this.nodePropertyKey = nodePropertyKey == null ? null : nodePropertyKey.toLowerCase();
        }
        else
        {
            throw new IllegalArgumentException(
                    "Can not create description with property on both relationship and node" );
        }
    }

    public String getDescription()
    {
        if ( description == null )
        {
            StringBuilder builder = new StringBuilder();
            builder.append( "(:" ).append( firstLabel ).append( ")" );
            String prefix;
            String suffix;

            if ( direction.equals( Direction.OUTGOING ) )
            {
                prefix = "";
                suffix = ">";
            }
            else if ( direction.equals( Direction.INCOMING ) )
            {
                prefix = "<";
                suffix = "";
            }
            else
            {
                prefix = suffix = "";
            }
            builder.append( prefix ).append( "[:" ).append( relationshipType );
            if ( relationshipPropertyKey != null )
            {
                appendProperty( builder, relationshipPropertyKey );
            }
            builder.append( "]" ).append( suffix ).append( "(:" ).append( secondLabel );
            if ( nodePropertyKey != null )
            {
                appendProperty( builder, nodePropertyKey );
            }
            builder.append( ")" );
            description = builder.toString();
        }
        return description;
    }

    private void appendProperty( StringBuilder builder, String propertyKey )
    {
        builder.append( "{" ).append( propertyKey ).append( ":}" );
    }

    @Override
    public int hashCode()
    {
        return getDescription().hashCode();
    }

    @Override
    public boolean equals( Object obj ) {
        if ( !( obj instanceof ShortcutIndexDescription ) )
            return false;
        if ( obj == this )
            return true;

        ShortcutIndexDescription rhs = (ShortcutIndexDescription) obj;
        return getDescription().equals( rhs.getDescription() );
    }

    @Override
    public String toString()
    {
        return getDescription();
    }
}
