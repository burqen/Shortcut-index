package bench.queries;

import bench.BaseQuery;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.impl.api.RelationshipDataExtractor;

public abstract class Query1 extends BaseQuery
{
    private String query = "MATCH (m:Person {name:\"Maria\"}) - [:CREATED] -> (c:Comment)";
    int personLabelId;
    int namePropertyKey;
    int createdTypeId;
    int commentLabelId;

    @Override
    public String query()
    {
        return query;
    }

    @Override
    protected void doRunQuery( ReadOperations operations, Measurement measurement )
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

    protected abstract void doTraverseFromStart( PrimitiveLongIterator startNodes, ReadOperations operations,
            Measurement measurement );
}
