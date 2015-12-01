package bench;

import java.util.concurrent.TimeUnit;

public class StupidTest
{
    public static void main( String[] args)
    {
        System.out.println( TimeUnit.MICROSECONDS.convert( 10, TimeUnit.SECONDS ) );
    }
}
