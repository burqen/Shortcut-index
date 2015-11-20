package index;

import java.util.List;

public interface SCResultVisitor
{
    boolean visit( long firstId, long keyProp, long relId, long secondId );

    long rowCount();

    void massageRawResult();

    public void limit();

    public static SCResultVisitor storeInListVisitor( List<SCResult> list )
    {
        return new SCResultVisitor()
        {
            @Override
            public boolean visit( long firstId, long keyProp, long relId, long secondId )
            {
                return list.add( new SCResult( new SCKey( firstId, keyProp ), new SCValue( relId, secondId ) ) );
            }

            @Override
            public long rowCount()
            {
                return list.size();
            }

            @Override
            public void massageRawResult()
            {
                // Do nothing
            }

            @Override
            public void limit()
            {
                // Do nothing
            }
        };
    }

    public class CountingResultVisitor implements SCResultVisitor
    {
        long rowCount;

        @Override
        public boolean visit( long firstId, long keyProp, long relId, long secondId )
        {
            rowCount++;
            return true;
        }

        @Override
        public long rowCount()
        {
            return rowCount;
        }

        @Override
        public void massageRawResult()
        {
            // Do nothing
        }

        @Override
        public void limit()
        {
            // Do nothing
        }
    }
}
