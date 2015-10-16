package bench.queries;

import bench.Environment;

public abstract class QueryDescription
{
    public abstract String queryName();

    public abstract String cypher();

    public abstract String[] inputDataHeader();

    public abstract String inputFile();

    public abstract Environment environment();

    @Override
    public int hashCode() {
        return queryName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if ( !( obj instanceof QueryDescription ) )
            return false;
        if ( obj == this )
            return true;

        QueryDescription rhs = (QueryDescription) obj;
        return queryName().equals( rhs.queryName() );
    }
}
