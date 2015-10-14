package bench.queries.framework;

import bench.Measurement;
import index.SCKey;
import index.SCResult;
import index.SCValue;

import java.util.List;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipDataExtractor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

public abstract class QueryKernelWithPropertyOnNode extends QueryKernel
{
    public QueryKernelWithPropertyOnNode()
    {
        super();
    }

    @Override
    protected void expandFromStart( ReadOperations operations, Measurement measurement, long[] inputData,
            long startPoint, int relType, int secondLabel, int propKey, List<SCResult> resultList )
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
                    long prop = ((Number) operations.nodeGetProperty( otherNode, propKey ) ).longValue();

                    if ( filterOnNodeProperty( prop ) )
                    {
                        continue;
                    }

                    SCResult result = new SCResult(
                            new SCKey( startPoint, prop ), new SCValue( relationship, otherNode ) );
                    if ( !filterResultRow( result ) )
                    {
                        // Valid result. Report
                        resultList.add( result );
                    }
                }
            }
        }
        catch( EntityNotFoundException e )
        {
            e.printStackTrace();
        }
    }

    protected abstract boolean filterOnNodeProperty( long prop );
}
