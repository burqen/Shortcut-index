package bench;

import org.HdrHistogram.Histogram;

import java.util.concurrent.TimeUnit;

public interface IndexBuildLogger
{
    void startInsert();
    void finishInsert();

    String report();
    String report( long pinsDuringClocking, long faultsDuringClocking );

    void setConfig( int cacheSize, int pageSize, int cachePages, int atLeastNumberOfPages, int atMostNumberOfPages,
            double cacheCoverage );

    public class HistogramIndexBuildLogger implements IndexBuildLogger
    {
        private Histogram histogram;
        private long startTime;
        private boolean inserting;
        private int cacheSize;
        private int pageSize;
        private int cachePages;
        private int atLeastNumberOfPages;
        private int atMostNumberOfPages;
        private double cacheCoverage;

        public HistogramIndexBuildLogger()
        {
            this.histogram = new Histogram( TimeUnit.MICROSECONDS.convert( 10, TimeUnit.SECONDS ), 5 );
        }

        public void startInsert()
        {
            if ( inserting )
            {
                return;
            }
            inserting = true;
            startTime = System.nanoTime();
        }

        public void finishInsert()
        {
            if ( !inserting )
            {
                return;
            }
            inserting = false;
            histogram.recordValue( (System.nanoTime() - startTime ) / 1000 );
        }

        public String report()
        {
            return String.valueOf( header() ) + result();
        }

        @Override
        public String report( long pinsDuringClocking, long faultsDuringClocking )
        {
            String header = header();
            String result = result();
            double hitRate = 100 - ( (double) faultsDuringClocking / pinsDuringClocking ) * 100;
            String pageCachePerformance = String.format( "%-25s : %10d \n", "Page pins", faultsDuringClocking ) +
                                          String.format( "%-25s : %10d \n", "Faults", pinsDuringClocking ) +
                                          String.format( "%-25s : %10.2f%% \n", "Hit rate", hitRate );
            return header + pageCachePerformance + result;
        }

        private String result()
        {
            return String.format( "%25s\n", "Insert time" ) +
                   String.format( "%25s : %10d\n", "COUNT", histogram.getTotalCount() ) +
                   String.format( "%25s : %10.0f µs\n", "MEAN", histogram.getMean() ) +
                   String.format( "%25s : %10d µs\n", "MIN", histogram.getMinValue() ) +
                   String.format( "%25s : %10d µs\n", "MAX", histogram.getMaxValue() ) +
                   String.format( "%25s : %10d µs\n", "50th PERCENTILE", histogram.getValueAtPercentile( 50 ) ) +
                   String.format( "%25s : %10d µs\n", "90th PERCENTILE", histogram.getValueAtPercentile( 90 ) ) +
                   String.format( "%25s : %10d µs\n", "95th PERCENTILE", histogram.getValueAtPercentile( 95 ) ) +
                   String.format( "%25s : %10d µs\n", "99th PERCENTILE", histogram.getValueAtPercentile( 99 ) );
        }

        private String header()
        {
            StringBuilder header = new StringBuilder();
            header.append( String.format( "%-25s : %,10d kB\n", "Cache size", cacheSize / 1000 ) )
                    .append( String.format( "%-25s : %10d B\n", "Page size", pageSize ) )
                    .append( String.format( "%-25s : %10d\n", "Cache pages", cachePages ) )
                    .append( String.format( "%-25s : %10d\n", "Pages in index at least", atLeastNumberOfPages) )
                    .append( String.format( "%-25s : %10d\n", "Pages in indes at most", atMostNumberOfPages ) )
                    .append( String.format( "%-25s : %10.2f%%\n", "Cache coverage", cacheCoverage ) );

            return header.toString();
        }

        @Override
        public void setConfig( int cacheSize, int pageSize, int cachePages, int atLeastNumberOfPages,
                int atMostNumberOfPages, double cacheCoverage )
        {
            this.cacheSize = cacheSize;
            this.pageSize = pageSize;
            this.cachePages = cachePages;
            this.atLeastNumberOfPages = atLeastNumberOfPages;
            this.atMostNumberOfPages = atMostNumberOfPages;
            this.cacheCoverage = cacheCoverage;
        }
    }

    public static IndexBuildLogger NULL = new IndexBuildLogger()
    {

        @Override
        public void startInsert()
        {
            // Do nothing
        }

        @Override
        public void finishInsert()
        {
            // Do nothing
        }

        @Override
        public String report()
        {
            return "Null logger report nothing";
        }

        @Override
        public String report( long pinsDuringClocking, long faultsDuringClocking )
        {
            return "Null logger report nothing";
        }

        @Override
        public void setConfig( int cacheSize, int pageSize, int cachePages, int atLeastNumberOfPages,
                int atMostNumberOfPages, double cacheCoverage )
        {
            // Do nothing
        }
    };
}
