package bench;

import org.HdrHistogram.Histogram;

public interface IndexBuildLogger
{
    void startInsert();
    void finishInsert();

    public class HistogramIndexBuildLogger implements IndexBuildLogger
    {
        private Histogram histogram;
        private long startTime;
        private boolean inserting;
        private long insertCount;

        public HistogramIndexBuildLogger( Histogram histogram )
        {
            this.histogram = histogram;
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
            insertCount++;
        }
    }

    public static IndexBuildLogger nullLogger()
    {
        return new IndexBuildLogger()
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
        };
    }
}
