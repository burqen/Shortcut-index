package bench;

import bench.queries.Measurement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;


import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class BenchLoggerTest
{
    private BenchLogger logger;

    @Before
    public void reset()
    {
        logger.reset();
    }

    @Test
    public void timing() throws InterruptedException
    {
        logger.start( "" );
        Thread.currentThread().sleep( 2000 );
        logger.end();
        long duration = logger.duration();
        assertTrue( "Expected duration to be 2000, was " + duration, Math.abs( duration - 2000 ) <= 10 );
    }

    @Test
    public void measurment()
    {
        logger.start( "" );
        Measurement measurement = logger.measurement();
        assertEquals( "Expected to not have any successes.", 0, measurement.getSuccesses() );
        measurement.countSuccesses();
        assertEquals( "Expected to have one (1) success.", 1, measurement.getSuccesses() );
        logger.end();
    }

    @Test( expected = IllegalStateException.class )
    public void reportOnNotStartedLogger()
    {
        logger.report();
    }

    @Test( expected = IllegalStateException.class )
    public void endOnNotStartedLogger()
    {
        logger.end();
    }

    @Test( expected = IllegalStateException.class )
    public void measurementOnNotStartedLogger()
    {
        logger.measurement();
    }

    @Parameterized.Parameters
    public static List<Object[]> balanceRates() {
        return Arrays.asList( new Object[][]{
                { new BenchLogger( Mockito.mock( PrintStream.class ) ) }
        } );
    }

    public BenchLoggerTest( BenchLogger logger )
    {
        this.logger = logger;
    }
}
