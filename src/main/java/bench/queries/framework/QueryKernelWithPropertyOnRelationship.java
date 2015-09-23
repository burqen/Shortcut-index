package bench.queries.framework;

import index.logical.TKey;
import index.logical.TResult;
import index.logical.TValue;

import java.util.List;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
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
            long startPoint, int relType, int secondLabel, int propKey, List<TResult> resultList )
    {

        try
        {
            RelationshipIterator relationships = operations.nodeGetRelationships( startPoint, direction(), relType );

            RelationshipDataExtractor extractor = new RelationshipDataExtractor();
            while ( relationships.hasNext() )
            {
                long relationship = relationships.next();

                long prop = ((Number) operations.relationshipGetProperty( relationship, propKey ).value() ).longValue();

                if ( filterOnRelationshipProperty( prop ) )
                {
                    continue;
                }

                operations.relationshipVisit( relationship, extractor );


                // Other node should now be a have second label and have a relationship of relType to startPoint
                long otherNode = startPoint == extractor.startNode() ? extractor.endNode() : extractor.startNode();
                if ( operations.nodeHasLabel( otherNode, secondLabel ) )
                {
                    TResult result = new TResult(
                            new TKey( startPoint, prop ), new TValue( relationship, otherNode ) );
                    if ( !filterResultRow( result ) )
                    {
                        // Valid result. Report
                        resultList.add( result );
                    }
                }
            }
        }
        catch(EntityNotFoundException e)
        {
            e.printStackTrace();
        }
        catch ( PropertyNotFoundException e )
        {
            e.printStackTrace();
        }
    }

    protected abstract boolean filterOnRelationshipProperty( long prop );
}

