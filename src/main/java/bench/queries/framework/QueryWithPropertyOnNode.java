package bench.queries.framework;

import bench.queries.Measurement;
import index.logical.ShortcutIndexProvider;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipDataExtractor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

public abstract class QueryWithPropertyOnNode extends BaseQuery
{
    public QueryWithPropertyOnNode()
    {
        super();
    }

    public QueryWithPropertyOnNode( ShortcutIndexProvider indexes )
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

                operations.relationshipVisit( relationship, extractor );

                // Other node should now be a comment written by person
                long otherNode = startPoint == extractor.startNode() ? extractor.endNode() : extractor.startNode();
                if ( operations.nodeHasLabel( otherNode, secondLabel ) )
                {
                    long prop = (long)operations.nodeGetProperty( otherNode, propKey ).value();

                    if ( filterOnNodeProperty( prop ) )
                    {
                        continue;
                    }

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

    protected abstract boolean filterOnNodeProperty( long prop );
}
