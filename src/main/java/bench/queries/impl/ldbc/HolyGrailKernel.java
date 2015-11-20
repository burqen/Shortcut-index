package bench.queries.impl.ldbc;

import bench.Measurement;
import bench.QueryType;
import index.SCIndexDescription;
import index.SCIndexProvider;
import index.SCKey;
import index.SCResult;
import index.SCResultVisitor;
import index.SCValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
            SCResultVisitor visitor, int propKey, int commentHasCreator, int commentLabel )
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
                visitor.visit( otherNode, prop, relationship, comment );
            }
        }
    }

    @Override
    protected SCResultVisitor getVisitor()
    {
        return new SCResultVisitor()
        {
            List<SCResult> list = new ArrayList<>();

            @Override
            public boolean visit( long firstId, long keyProp, long relId, long secondId )
            {
                return list.add( new SCResult( new SCKey( firstId, keyProp ), new SCValue( relId, secondId ) ) );
            }

            @Override
            public long rowCount()
            {
                return list.size();
            }

            @Override
            public void massageRawResult()
            {
                Collections.sort( list, ( o1, o2 ) -> -Long.compare( o1.getKey().getProp(), o2.getKey().getProp() ) );
            }

            @Override
            public void limit()
            {
                if ( list.size() > 20 )
                {
                    list = list.subList( 0, 20 );
                }
            }
        };
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
