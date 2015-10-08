package index;

import index.legacy.LegacySCIndex;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Direction;

public class ShortcutIndexProvider
{
    Map<SCIndexDescription,LegacySCIndex> indexes;

    public ShortcutIndexProvider()
    {
        indexes = new HashMap<>();
    }

    public SCIndex get( String firstLabel,
            String secondLabel,
            String relationshipType,
            Direction direction,
            String relationshipPropertyKey,
            String nodePropertyKey )
    {
        return get( new SCIndexDescription( firstLabel, secondLabel, relationshipType,
                direction, relationshipPropertyKey, nodePropertyKey ) );
    }
    public LegacySCIndex get( SCIndexDescription description )
    {
        return indexes.get( description );
    }

    public void put( LegacySCIndex index )
    {
        indexes.put( index.getDescription(), index );
    }
}
