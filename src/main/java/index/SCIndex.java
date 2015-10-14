package index;

import java.io.IOException;
import java.util.List;

public interface SCIndex
{
    SCIndexDescription getDescription();

    void insert( long[] key, long[] value ) throws IOException;

    void seek( Seeker seeker, List<SCResult> resultList) throws IOException;
}
