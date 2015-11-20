package bench.queries.impl.framework;

import bench.Measurement;
import index.SCResultVisitor;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipDataExtractor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

public abstract class QueryKernelWithPropertyOnRelationship extends QueryKernel
{
    public QueryKernelWithPropertyOnRelationship()
    {
        super();
    }

    @Override
    protected void expandFromStart( ReadOperations operations, Measurement measurement, long[] inputData,
            long startPoint, int relType, int secondLabel, int propKey, SCResultVisitor visitor )
    {

        try
        {
            RelationshipIterator relationships = operations.nodeGetRelationships( startPoint, direction(), relType );

            RelationshipDataExtractor extractor = new RelationshipDataExtractor();
            while ( relationships.hasNext() )
            {
                long relationship = relationships.next();

                long prop = ((Number) operations.relationshipGetProperty( relationship, propKey ) ).longValue();

                if ( filterOnRelationshipProperty( prop ) )
                {
                    continue;
                }

                operations.relationshipVisit( relationship, extractor );


                // Other node should now be a have second label and have a relationship of relType to startPoint
                long otherNode = startPoint == extractor.startNode() ? extractor.endNode() : extractor.startNode();
                if ( operations.nodeHasLabel( otherNode, secondLabel ) )
                {
                    visitor.visit( startPoint, prop, relationship, otherNode );
                }
            }
        }
        catch(EntityNotFoundException e)
        {
            e.printStackTrace();
        }
    }

    protected abstract boolean filterOnRelationshipProperty( long prop );
}

