package index.logical;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ShortcutIndexServiceInsertTest
{
    int order = 2;
    ShortcutIndexService index;
    Random rng = new Random();
    @Mock
    ShortcutIndexDescription desc;

    @Before
    public void setUpIndexClass()
    {
        index = new ShortcutIndexService( order, desc );

    }

    @Test
    public void singleInsert()
    {
        index.insert( randomKey(), randomValue() );
    }

    @Test
    // TODO: This test can only be checked manually for now.
    public void insertsInOrder()
    {
        int numberOfKeys = 100;
        TKey[] keys = new TKey[numberOfKeys];
        for ( int i = 0; i < numberOfKeys; i++ )
        {
            keys[i] = new TKey( i, i );
            index.insert( keys[i], randomValue() );
        }
        index.printTree( System.out );
    }

    @Test
    // TODO: This test can only be checked manually for now.
    public void insertsOutOfOrder()
    {
        int numberOfKeys = 100;
        for ( int i = 0; i < numberOfKeys; i++ )
        {
            int key = rng.nextInt( 1000 );
            index.insert( new TKey( key, key ), randomValue() );
        }
        index.printTree( System.out );
    }

    @Test
    public void multipleInserts()
    {
        for ( int i = 0; i < 1000; i++ )
        {
            index.insert( randomKey(), randomValue() );
        }

        long keyCount = index.totalKeyCount();

        assertEquals( 1000, keyCount );
    }

    @Test
    public void height()
    {
        int order = 2;
        index = new ShortcutIndexService( order, desc );

        int maxNumberOfKeys;
        for ( int i = 0; i < 1000; i++ )
        {
            index.insert( randomKey(), randomValue() );

            maxNumberOfKeys = maxNumberOfKeys( order, index.height() );
            assertTrue( "Expected " + (i+1) + " <= " + maxNumberOfKeys, i + 1 <= maxNumberOfKeys );
        }

    }

    private TKey randomKey()
    {
        return new TKey( rng.nextLong(), rng.nextLong() );
    }

    private TValue randomValue()
    {
        return new TValue( rng.nextLong(), rng.nextLong() );
    }

    private int maxNumberOfKeys( int order, int height )
    {
        if ( height < 0 )
        {
            return 0;
        }
        return 2 * order * (int)Math.pow( 2 * order + 1, height );
    }
}
