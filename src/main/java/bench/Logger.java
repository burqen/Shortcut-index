package bench;

import bench.queries.QueryDescription;
import org.HdrHistogram.Histogram;

public interface Logger
{
    public Measurement startQuery( QueryDescription queryDescription, QueryType queryType );
    public void report( LogStrategy logStrategy );

    public static Logger DUMMY_LOGGER = new Logger()
    {
        @Override
        public Measurement startQuery( QueryDescription queryDescription, QueryType queryType )
        {
            return new Measurement()
            {
                @Override
                public void queryFinished( long elapsedTime, long rowCount )
                {

                }

                @Override
                public boolean error()
                {
                    return false;
                }

                @Override
                public String errorMessage()
                {
                    return null;
                }

                @Override
                public void error( String s )
                {

                }

                @Override
                public Histogram timeHistogram()
                {
                    return null;
                }

                @Override
                public Histogram rowHistogram()
                {
                    return null;
                }

                @Override
                public void firstQueryFinished( long elapsedTime )
                {

                }

                @Override
                public void lastQueryFinished( long elapsedTime )
                {

                }

                @Override
                public long timeForFirstQuery()
                {
                    return 0;
                }

                @Override
                public long timeForLastQuery()
                {
                    return 0;
                }

                @Override
                public long[] completeTimesLog()
                {
                    return new long[0];
                }
            };
        }

        @Override
        public void report( LogStrategy logStrategy )
        {

        }

        @Override
        public void close()
        {

        }
    };

    void close();

}
