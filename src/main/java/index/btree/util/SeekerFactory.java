package index.btree.util;

import index.Seeker;
import index.btree.RangePredicate;
import index.btree.RangeSeeker;
import index.btree.Scanner;

public class SeekerFactory
{
    public static Seeker scanner()
    {
        return new Scanner();
    }

    public static Seeker exactMatch( long id, long prop )
    {
        return new RangeSeeker( RangePredicate.equalTo( id, prop ), RangePredicate.equalTo( id, prop ) );
    }
}
