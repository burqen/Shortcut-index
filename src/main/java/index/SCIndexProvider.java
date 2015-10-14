package index;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Direction;

public class SCIndexProvider
{
    Map<SCIndexDescription,SCIndex> indexes;

    public SCIndexProvider()
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
    public SCIndex get( SCIndexDescription description )
    {
        return indexes.get( description );
    }

    public void put( SCIndex index )
    {
        indexes.put( index.getDescription(), index );
    }
}
