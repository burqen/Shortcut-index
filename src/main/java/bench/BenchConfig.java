package bench;

public class BenchConfig
{
    private final int pageSize;
    private final int cachePages;
    private final int inputSize;
    private final int nbrOfWarmup;

    public BenchConfig( int pageSize, int cachePages, int inputSize, int nbrOfWarmup )
    {
        this.pageSize = pageSize;
        this.cachePages = cachePages;
        this.inputSize = inputSize;
        this.nbrOfWarmup = nbrOfWarmup;
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
}
