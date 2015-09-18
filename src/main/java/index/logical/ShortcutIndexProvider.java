package index.logical;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Direction;

public class ShortcutIndexProvider
{
    Map<ShortcutIndexDescription, ShortcutIndexService> indexes;

    public ShortcutIndexProvider()
    {
        indexes = new HashMap<>();
    }

    public ShortcutIndexService get( String firstLabel,
            String secondLabel,
            String relationshipType,
            Direction direction,
            String relationshipPropertyKey,
            String nodePropertyKey )
    {
        return get( new ShortcutIndexDescription( firstLabel, secondLabel, relationshipType,
                direction, relationshipPropertyKey, nodePropertyKey ) );
    }
    public ShortcutIndexService get( ShortcutIndexDescription description )
    {
        return indexes.get( description );
    }

    public void put( ShortcutIndexService index )
    {
        indexes.put( index.getDescription(), index );
    }
}
