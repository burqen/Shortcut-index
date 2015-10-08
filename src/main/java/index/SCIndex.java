package index;

import java.io.IOException;

public interface SCIndex
{
    SCIndexDescription getDescription();

    void insert( long[] key, long[] value ) throws IOException;
}
