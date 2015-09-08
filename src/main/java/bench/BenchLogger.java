package bench;

import bench.queries.Measurement;

import java.io.PrintStream;

public class BenchLogger
{
    private final Measurement measurement = new Measurement()
    {
        private long successes = 0;

        public void countSuccesses()
        {
            successes++;
        }

        public long getSuccesses()
        {
            return successes;
        }

        public void reset()
        {
            successes = 0;
        }
    };
    private final PrintStream out;
    private long startTime = 0;
    private long duration = 0;
    private String query;

    public BenchLogger( PrintStream out ) {
        this.out = out;
    }

    public void start( String query )
    {
        this.query = query;
        startTime = System.currentTimeMillis();
    }

    public void end()
    {
        if ( startTime == 0 )
        {
            throw new IllegalStateException( "Need to start log before calling end" );
        }
        duration = System.currentTimeMillis() - startTime;
    }

    public Measurement measurement()
    {
        if ( startTime == 0 )
        {
            throw new IllegalStateException( "Need to start before counting successes" );
        }
        return measurement;
    }

    public void report()
    {
        if ( startTime == 0 || duration == 0 )
        {
            throw new IllegalStateException( "Need to start and end log before reporting" );
        }
        out.print( String.format( "%s\n\t%-15s\t%-15s\n\t%-15d\t%-15d\n",
                query, "Time (ms)", "Successes", duration, measurement.getSuccesses() ) );
    }

    public void reset()
    {
        measurement.reset();
        startTime = 0;
        duration = 0;
        query = "";
    }
}
