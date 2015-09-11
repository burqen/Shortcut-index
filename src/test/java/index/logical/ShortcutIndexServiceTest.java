package index.logical;

import org.junit.Before;
import org.junit.Test;

public class ShortcutIndexServiceTest
{
    int order = 2;
    ShortcutIndexService index;

    @Before
    public void setUpIndexClass()
    {
        index = new ShortcutIndexService( order );

    }

    @Test
    public void singleInsert()
    {
        long idFirstNode = System.currentTimeMillis();
        Long prop = System.currentTimeMillis();
        long idRel = System.currentTimeMillis();
        long idOtherNode = System.currentTimeMillis();
        index.insert( new TKey( idFirstNode, prop ), new TValue( idRel, idOtherNode ) );
    }

    @Test
    public void multipleInserts()
    {
        for ( int i = 0; i < 1000; i++ )
        {
            long idFirstNode = System.currentTimeMillis();
            Long prop = System.currentTimeMillis();
            long idRel = System.currentTimeMillis();
            long idOtherNode = System.currentTimeMillis();
            index.insert( new TKey( idFirstNode, prop ), new TValue( idRel, idOtherNode ) );
        }
    }
}
