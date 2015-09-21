package bench;

import bench.queries.Measurement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class BenchLoggerTest
{
    private BenchLogger logger;

    @Test
    public void error()
    {
        throw new NotImplementedException();
    }

    // TODO: Implement me
//    @Test
//    public void measurment()
//    {
//        logger.startQuery( "" );
//        Measurement measurement = logger.measurement();
//        assertEquals( "Expected to not have any successes.", 0, measurement.getSuccesses() );
//        measurement.countSuccess();
//        assertEquals( "Expected to have one (1) success.", 1, measurement.getSuccesses() );
//        logger.finishQuery();
//    }

    @Test( expected = IllegalStateException.class )
    public void reportOnNotStartedLogger()
    {
        logger.report();
    }

    @Parameterized.Parameters
    public static List<Object[]> loggers() {
        return Arrays.asList( new Object[][]{
                { new BenchLogger( Mockito.mock( PrintStream.class ) ) }
        } );
    }

    public BenchLoggerTest( BenchLogger logger )
    {
        this.logger = logger;
    }
}
