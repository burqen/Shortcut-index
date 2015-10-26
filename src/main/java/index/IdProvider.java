package index;

import java.io.FileNotFoundException;

public interface IdProvider
{
    long acquireNewId() throws FileNotFoundException;
}
