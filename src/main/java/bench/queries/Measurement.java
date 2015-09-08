package bench.queries;

public interface Measurement
{
    void countSuccesses();
    long getSuccesses();
    void reset();
}
