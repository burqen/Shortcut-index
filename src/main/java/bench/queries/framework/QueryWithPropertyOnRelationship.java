package bench.queries.framework;

import bench.queries.Measurement;
import bench.queries.framework.BaseQuery;
import index.logical.ShortcutIndexProvider;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipDataExtractor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

public abstract class QueryWithPropertyOnRelationship extends BaseQuery
{
    public QueryWithPropertyOnRelationship()
    {
        super();
    }

    public QueryWithPropertyOnRelationship( ShortcutIndexProvider indexes )
    {
        super( indexes );
    }

    @Override
    protected void expandFromStart( ReadOperations operations, Measurement measurement, long[] inputData,
            long startPoint, int relType, int secondLabel, int propKey )
    {

        try
        {
            RelationshipIterator relationships = operations.nodeGetRelationships( startPoint, direction(), relType );

            RelationshipDataExtractor extractor = new RelationshipDataExtractor();
            while ( relationships.hasNext() )
            {
                long relationship = relationships.next();

                long prop = (long) operations.relationshipGetProperty( relationship, propKey ).value();

                if ( filterOnRelationshipProperty( prop ) )
                {
                    continue;
                }

                operations.relationshipVisit( relationship, extractor );


                // Other node should now be a comment written by person
                long otherNode = startPoint == extractor.startNode() ? extractor.endNode() : extractor.startNode();
                if ( operations.nodeHasLabel( otherNode, secondLabel ) )
                {
                    if ( validateRow( startPoint, otherNode, relationship, prop ) )
                    {
                        // Valid result. Report
                        measurement.row();
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

