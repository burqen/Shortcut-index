package bench.queries.impl.ldbc;

import bench.queries.QueryDescription;
import bench.queries.framework.QueryKernelWithPropertyOnNode;
import bench.queries.impl.description.Query2Description;
import bench.util.Config;
import bench.util.SingleEntryPrimitiveLongIterator;
import index.SCResult;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class Query2Kernel extends QueryKernelWithPropertyOnNode
{
    public Query2Kernel()
    {
        super();
    }

    @Override
    protected String firstLabel()
    {
        return "Person";
    }

    @Override
    protected String secondLabel()
    {
        return "Comment";
    }

    @Override
    protected String relType()
    {
        return "COMMENT_HAS_CREATOR";
    }

    @Override
    protected Direction direction()
    {
        return Direction.INCOMING;
    }

    @Override
    protected String propKey()
    {
        return "creationDate";
    }

    @Override
    protected boolean filterOnNodeProperty( long prop )
    {
        return false;
    }

    @Override
    public String[] inputDataHeader()
    {
        return new String[]{ "Person" };
    }

    @Override
    public String cypher()
    {
        return "// QUERY 2 - SEEK\n" +
               "// All comments written by person\n" +
               "MATCH (p:Person {id:{1}}) <-[r:COMMENT_HAS_CREATOR]- (c:Comment)\n" +
               "RETURN id(p), id(r), id(c), c.creationDate\n";
    }

    @Override
    protected PrimitiveLongIterator startingPoints( ReadOperations operations, long[] inputData, int firstLabel )
            throws EntityNotFoundException
    {
        if ( operations.nodeHasLabel( inputData[0], firstLabel ) )
        {
            return new SingleEntryPrimitiveLongIterator( inputData[0] );
        }
        else
        {
            throw new IllegalArgumentException(
                    "Node[" + inputData[0] + "] did not have label " + firstLabel() + " as expected. " +
                    "Use correct input file." );
        }
    }

    @Override
    protected boolean filterResultRow( SCResult resultRow )
    {
        return false;
    }

    @Override
    public QueryDescription queryDescription()
    {
        return Query2Description.instance;
    }
}
