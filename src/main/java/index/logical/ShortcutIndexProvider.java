package index.logical;

import java.util.HashMap;
import java.util.Map;

public class ShortcutIndexProvider
{
    Map<ShortcutIndexDescription, ShortcutIndexService> indexes;
    ShortcutIndexService tmp;

    public ShortcutIndexProvider()
    {
        indexes = new HashMap<>();
    }

    public ShortcutIndexService get( ShortcutIndexDescription description )
    {
//        return indexes.get( description );
        return tmp;
    }

    public void put( ShortcutIndexService index )
    {
        //indexes.put( index.getDescription(), index );
        tmp = index;
    }
}
