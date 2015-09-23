package bench.queries.framework;

public interface QueryDescription
{
    String cypher();

    String[] inputDataHeader();

    String inputFile();
}
