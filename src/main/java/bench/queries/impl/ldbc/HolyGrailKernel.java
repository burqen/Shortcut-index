package bench.queries.impl.ldbc;

import bench.Measurement;
import bench.QueryType;
import index.SCIndexDescription;
import index.SCIndexProvider;
import index.SCKey;
import index.SCResult;
import index.SCValue;

import java.io.IOException;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipDataExtractor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

public class HolyGrailKernel extends AbstractHolyGrail
{
    public HolyGrailKernel( long limit )
    {
        super( limit );
    }

    @Override
    protected void lastHop( ReadOperations operations, Measurement measurement, long[] inputData, long otherNode,
            List<SCResult> queryResult, int propKey, int commentHasCreator, int commentLabel )
            throws IOException, EntityNotFoundException
    {
        RelationshipIterator relationships = operations.nodeGetRelationships( otherNode, Direction.BOTH,
                commentHasCreator );

        RelationshipDataExtractor extractor = new RelationshipDataExtractor();
        while ( relationships.hasNext() )
        {
            long relationship = relationships.next();

            operations.relationshipVisit( relationship, extractor );

            // Other node should now be a comment written by person
            long comment = otherNode == extractor.startNode() ? extractor.endNode() : extractor.startNode();
            if ( operations.nodeHasLabel( comment, commentLabel ) )
            {
                long prop = ((Number) operations.nodeGetProperty( otherNode, propKey )).longValue();

                if ( prop > limit )
                {
                    return;
                }

                SCResult result = new SCResult(
                        new SCKey( otherNode, prop ), new SCValue( relationship, comment ) );
                // Valid result. Report
                queryResult.add( result );
            }
        }
    }

    @Override
    public SCIndexDescription indexDescription()
    {
        return null;
    }

    @Override
    public QueryType type()
    {
        return QueryType.KERNEL;
    }

    @Override
    public void setIndexProvider( SCIndexProvider indexes )
    {
        // Do nothing
    }
}
