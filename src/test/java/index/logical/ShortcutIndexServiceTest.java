package index.logical;

import org.junit.Test;
import org.mockito.Mock;

public class ShortcutIndexServiceTest
{
    @Mock
    ShortcutStorage storage;
    ShortcutIndexService<TKey<Long>, TValue> index = new ShortcutIndexService( TKey.class, TValue.class, 2 );

    @Test
    public void insertOneLeaf()
    {
        for ( int i = 0; i < 6; i++ )
        {
            long idFirstNode = System.currentTimeMillis();
            Long prop = System.currentTimeMillis();
            long idRel = System.currentTimeMillis();
            long idOtherNode = System.currentTimeMillis();
            index.insert( new TKey( idFirstNode, prop ), new TValue( idRel, idOtherNode ) );
        }
    }
}
