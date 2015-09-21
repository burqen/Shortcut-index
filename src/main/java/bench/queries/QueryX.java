package bench.queries;

import bench.BaseQuery;
import index.logical.ShortcutIndexDescription;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;

public abstract class QueryX extends BaseQuery
{
    public static ShortcutIndexDescription indexDescription = new ShortcutIndexDescription( "Person", "Comment",
        "COMMENT_HAS_CREATOR", Direction.INCOMING, null, "creationDate" );
    int personLabelId;
    int namePropertyKey;
    int createdTypeId;
    int commentLabelId;

    @Override
    public String query()
    {
        return "MATCH (m:Person {name:\"Maria\"}) - [:CREATED] -> (c:Comment)";
    }

    @Override
    protected void doRunQuery( ReadOperations operations, Measurement measurement, long[] inputData )
    {
        personLabelId = operations.labelGetForName( "Person" );
        namePropertyKey = operations.propertyKeyGetForName( "firstName" );
        createdTypeId = operations.relationshipTypeGetForName( "COMMENT_HAS_CREATOR" );
        commentLabelId = operations.labelGetForName( "Comment" );

        try
        {
            PrimitiveLongIterator startNodes = getNodeFromIndexLookup( operations,
                    personLabelId,
                    namePropertyKey,
                    "Maria" );
            doTraverseFromStart( startNodes, operations, measurement );
        }
        catch ( IndexNotFoundKernelException e )
        {
            e.printStackTrace();
        }
    }

    @Override
    public String[] inputDataHeader()
    {
        return NO_HEADER;
    }

    protected abstract void doTraverseFromStart( PrimitiveLongIterator startNodes, ReadOperations operations,
            Measurement measurement );
}
