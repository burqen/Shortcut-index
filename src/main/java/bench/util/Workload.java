package bench.util;

import bench.Environment;
import bench.queries.Query;
import index.SCIndexDescription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Workload
{
    private Environment environment;
    private List<Query> queries;
    private Set<SCIndexDescription> indexDescriptions;

    public Workload( Environment environment )
    {
        this.environment = environment;
        this.queries = new ArrayList<>();
        this.indexDescriptions = new HashSet<>();
    }

    public boolean addQuery( Query query )
    {
        if ( query.environment() == environment )
        {
            return queries.add( query );
        }
        else
        {
            return false;
        }
    }

    public void buildIndexDescriptions()
    {
        for ( Query query : queries )
        {
            SCIndexDescription description = query.indexDescription();
            if ( description != null )
            {
                indexDescriptions.add( description );
            }
        }
    }

    public List<Query> queries()
    {
        return queries;
    }

    public boolean addQueries( Query[] queryArray )
    {
        return queries.addAll( Arrays.asList( queryArray ) );
    }

    public Iterator<SCIndexDescription> indexDescriptions()
    {
        return indexDescriptions.iterator();
    }
}
