package bench.queries;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipDataExtractor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

import static org.neo4j.graphdb.Direction.INCOMING;

public class QueryXKernel extends QueryX
{
    @Override
    protected void doTraverseFromStart( PrimitiveLongIterator startNodes, ReadOperations operations,
            Measurement measurement )
    {
        try
        {
            while ( startNodes.hasNext() )
            {
                final long maria = startNodes.next();

                RelationshipIterator createdIterator;
                createdIterator = operations.nodeGetRelationships( maria, INCOMING, createdTypeId );

                RelationshipDataExtractor visitor = new RelationshipDataExtractor();
                while ( createdIterator.hasNext() )
                {
                    long relId = createdIterator.next();
                    try
                    {
                        operations.relationshipVisit( relId, visitor );
                    }
                    catch ( Exception e )
                    {
                        e.printStackTrace();
                    }
                    // Other node should now be a comment written by Maria
                    long otherNode = maria == visitor.startNode() ? visitor.endNode(): visitor.startNode();
                    if ( operations.nodeHasLabel( otherNode, commentLabelId ) )
                    {
                        // Valid result. Report
                        measurement.row();
                    }
                }
            }
        }
        catch ( EntityNotFoundException e )
        {
            e.printStackTrace();
        }
    }
}
