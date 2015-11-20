package bench.queries.impl.ldbc;

import bench.Measurement;
import bench.queries.Query;
import bench.queries.QueryDescription;
import bench.queries.impl.description.HolyGrailDescription;
import index.SCResultVisitor;

import java.io.IOException;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipDataExtractor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

public abstract class AbstractHolyGrail extends Query
{
    protected final long limit;

    public AbstractHolyGrail( long limit )
    {
        this.limit = limit;
    }
    @Override
    public QueryDescription queryDescription()
    {
        return HolyGrailDescription.INSTANCE;
    }

    @Override
    protected long doRunQuery( ReadOperations operations, Measurement measurement, long[] inputData )
            throws IOException, EntityNotFoundException
    {
        int propKey = operations.propertyKeyGetForName( "creationDate" );
        int knows = operations.relationshipTypeGetForName( "KNOWS" );
        int commentHasCreator = operations.relationshipTypeGetForName( "COMMENT_HAS_CREATOR" );
        int commentLabel = operations.labelGetForName( "Comment" );
        int personLabel = operations.labelGetForName( "Person" );

        SCResultVisitor visitor = getVisitor();

        try
        {
            final long start = inputData[0];

            if ( !operations.nodeHasLabel( start, personLabel ) )
            {
                throw new IllegalArgumentException(
                        "Node[" + start + "] did not have label Person as expected. " +
                        "Use correct input file." );
            }

            RelationshipIterator relationships = operations.nodeGetRelationships( start, Direction.BOTH, knows );

            RelationshipDataExtractor extractor = new RelationshipDataExtractor();
            while ( relationships.hasNext() )
            {
                long relationship = relationships.next();

                operations.relationshipVisit( relationship, extractor );

                // Other node should now be a comment written by person
                long otherNode = start == extractor.startNode() ? extractor.endNode() : extractor.startNode();
                if ( operations.nodeHasLabel( otherNode, personLabel ) )
                {
                    // Last hop
                    lastHop( operations, measurement, inputData, otherNode, visitor, propKey, commentHasCreator,
                            commentLabel );
                }
            }
        }
        catch ( EntityNotFoundException e )
        {
            e.printStackTrace();
        }

        visitor.massageRawResult();
        visitor.limit();
        return visitor.rowCount();
    }

    protected abstract void lastHop( ReadOperations operations, Measurement measurement, long[] inputData,
            long otherNode, SCResultVisitor visitor, int propKey, int commentHasCreator, int commentLabel )
            throws IOException, EntityNotFoundException;
}
