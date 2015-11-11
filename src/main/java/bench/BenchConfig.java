package bench;

public class BenchConfig
{
    private final int pageSize;
    private final int cachePages;
    private final int inputSize;
    private final int nbrOfWarmup;
    private final String inputDataDir;

    public BenchConfig( int pageSize, int cachePages, int inputSize, int nbrOfWarmup, String inputDataDir )
    {
        this.pageSize = pageSize;
        this.cachePages = cachePages;
        this.inputSize = inputSize;
        this.nbrOfWarmup = nbrOfWarmup;
        this.inputDataDir = inputDataDir;
    }

    public int pageSize()
    {
        return pageSize;
    }

    public int inputSize()
    {
        return inputSize;
    }

    public int numberOfWarmups()
    {
        return nbrOfWarmup;
    }

    public int cachePages()
    {
        return cachePages;
    }

    public String inputDataDir()
    {
        return inputDataDir;
    }
}
