package bench;

import bench.util.LogCompleteHistogram;
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
