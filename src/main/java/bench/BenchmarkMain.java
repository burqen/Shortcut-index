package bench;

import bench.queries.Measurement;
import bench.queries.Query1;
import bench.queries.Query2;
import bench.queries.framework.BaseQuery;
import bench.util.Config;
import bench.util.GraphDatabaseProvider;
import bench.util.InputDataLoader;
import index.logical.ShortcutIndexDescription;
import index.logical.ShortcutIndexProvider;
import index.logical.ShortcutIndexService;
import index.logical.TKey;
import index.logical.TValue;

import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.tooling.GlobalGraphOperations;

import static bench.util.Config.GRAPH_DB_FOLDER;

public class BenchmarkMain
{
    private String dbName;

    public static void main( String[] argv ) throws FileNotFoundException
    {
        new BenchmarkMain().run( argv );
    }

    private void run( String[] argv ) throws FileNotFoundException
    {
        dbName = Config.LDBC_SF001;

        GraphDatabaseService graphDb = GraphDatabaseProvider.openDatabase( GRAPH_DB_FOLDER, dbName );

        if ( argv[0].equals( "bench" ) )
        {
            benchRun( graphDb );
        }
        else if (argv[0].equals( "alt" ) )
        {
            try ( Transaction tx = graphDb.beginTx() )
            {
                System.out.println( "ALL PROPERTY KEYS" );
                ResourceIterable<String> allPropertyKeys = GlobalGraphOperations.at( graphDb ).getAllPropertyKeys();
                for ( String propKey : allPropertyKeys )
                {
                    System.out.println( propKey );
                }
                System.out.println();

                System.out.println( "ALL RELATIONSHIP TYPES" );
                for ( RelationshipType next : GlobalGraphOperations.at( graphDb ).getAllRelationshipTypes() )
                {
                    System.out.println( next.name() );
                }
                System.out.println();

                System.out.println( "ALL COMMENT" );
                printNodesWithLabel( graphDb, "Comment" );

                tx.success();
            }

        }
    }

    private void benchRun( GraphDatabaseService graphDb ) throws FileNotFoundException
    {
        int order = 64;
        ShortcutIndexDescription description = Query1.indexDescription;
        ShortcutIndexService index = new ShortcutIndexService( order, description );

        populateShortcutIndex( graphDb, index,
                "Person", "COMMENT_HAS_CREATOR", Direction.INCOMING, "Comment", "creationDate", false );

        ShortcutIndexProvider indexes = new ShortcutIndexProvider();
        indexes.put( index );

        initiateLuceneIndex( graphDb, "Person", "firstName" );

        // Logger
        BenchLogger logger = new BenchLogger( System.out, " -~~- KERNEL -~~-" );

        // --- WITH KERNEL ---
        runKernelQueries( graphDb, logger );

        logger = new BenchLogger( System.out, "-~~- SHORTCUT -~~-" );

        // --- WITH SHORTCUT ---
        runShortcutQueries( graphDb, logger, indexes );
    }

    private void runKernelQueries( GraphDatabaseService graphDb, BenchLogger logger ) throws FileNotFoundException
    {
        // *** QUERY 1 ***
        BaseQuery query1 = new Query1();
        benchmarkQuery( query1, logger, graphDb, query1.inputFile() );

        // *** QUERY 2 ***
        BaseQuery query2 = new Query2();
        benchmarkQuery( query2, logger, graphDb, query2.inputFile() );

        logger.report();
    }

    private void runShortcutQueries( GraphDatabaseService graphDb, BenchLogger logger, ShortcutIndexProvider indexes )
            throws FileNotFoundException
    {
        // *** QUERY 1 ***
        BaseQuery query1 = new Query1( indexes );
        benchmarkQuery( query1, logger, graphDb, query1.inputFile() );

        // *** QUERY 2 ***
        BaseQuery query2 = new Query2( indexes );
        benchmarkQuery( query2, logger, graphDb, query2.inputFile() );

        logger.report();
    }

    private void benchmarkQuery(
            BaseQuery query, BenchLogger logger, GraphDatabaseService graphDb, String dataFileName )
            throws FileNotFoundException
    {
        // Load input data
        InputDataLoader inputDataLoader = new InputDataLoader();
        List<long[]> inputData = inputDataLoader.load( dataFileName, query.inputDataHeader() );
        if ( inputData == null )
        {
            Measurement measurement = logger.startQuery( query.query() );
            measurement.error( "Failed to load input data" );
        }
        else
        {
            // Start logging
            Measurement measurement = logger.startQuery( query.query() );

            // Run query
            for ( long[] input : inputData )
            {
                query.runQuery( graphDb, measurement, input );
            }
            measurement.close();
        }
    }

    private void populateShortcutIndex( GraphDatabaseService graphDb, ShortcutIndexService index,
            ShortcutIndexDescription desc )
    {
        if ( desc.nodePropertyKey != null )
        {
            populateShortcutIndex( graphDb, index, desc.firstLabel, desc.relationshipType, desc.direction,
                    desc.secondLabel, desc.nodePropertyKey, false );

        }
        else
        {
            populateShortcutIndex( graphDb, index, desc.firstLabel, desc.relationshipType, desc.direction,
                    desc.secondLabel, desc.relationshipPropertyKey, true );
        }
    }

    private void populateShortcutIndex( GraphDatabaseService graphDb, ShortcutIndexService index, String firstLabelName,
            String relTypeName, Direction dir, String secondLabelName, String propName, boolean propOnRel )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            Label firstLabel = DynamicLabel.label( firstLabelName );
            Label secondLabel = DynamicLabel.label( secondLabelName );

            Iterator<Relationship> allRelationships =
                    GlobalGraphOperations.at( graphDb ).getAllRelationships().iterator();
            int numberOfInsert = 0;
            int numberOfRelationships = 0;
            while ( allRelationships.hasNext() )
            {
                Relationship rel = allRelationships.next();
                numberOfRelationships++;
                if ( rel.getType().name().equals( relTypeName ) )
                {
                    Node first;
                    Node second;
                    if ( dir == Direction.OUTGOING )
                    {
                        first = rel.getStartNode();
                        second = rel.getEndNode();
                    }
                    else
                    {
                        first = rel.getEndNode();
                        second = rel.getStartNode();
                    }
                    if ( first.hasLabel( firstLabel ) && second.hasLabel( secondLabel ) )
                    {
                        numberOfInsert++;
                        long prop = propOnRel ? (long) rel.getProperty( propName ) :
                                          (long) second.getProperty( propName );
                        index.insert( new TKey( first.getId(), prop ), new TValue( rel.getId(), second.getId() ) );
                    }
                }
                if ( numberOfRelationships % 10000 == 0 )
                {
                    System.out.printf( "# relationships: %d, # inserts: %d\n", numberOfRelationships, numberOfInsert );
                }
            }
            tx.success();
        }
    }

    private void initiateLuceneIndex( GraphDatabaseService graphDb, String label, String prop )
    {

        boolean indexAlreadyExist = false;
        try ( Transaction tx = graphDb.beginTx() )
        {
            Schema schema = graphDb.schema();

            Iterable<IndexDefinition> indexes = schema.getIndexes( DynamicLabel.label( label ) );

            for ( IndexDefinition id : indexes )
            {
                Iterable<String> propertyKeys = id.getPropertyKeys();
                for ( String propKey : propertyKeys )
                {
                    if ( !propKey.equals( prop ) )
                    {
                        indexAlreadyExist = false;
                        break;
                    }
                    else
                    {
                        indexAlreadyExist = true;
                    }
                }
                if ( indexAlreadyExist )
                {
                    break;
                }
            }

            tx.success();
        }

        if ( !indexAlreadyExist )
        {
            IndexDefinition indexDefinition;
            try ( Transaction tx = graphDb.beginTx() )
            {
                Schema schema = graphDb.schema();
                indexDefinition = schema.indexFor( DynamicLabel.label( label ) )
                        .on( prop )
                        .create();

                tx.success();
            }

            try ( Transaction tx = graphDb.beginTx() )
            {
                Schema schema = graphDb.schema();
                schema.awaitIndexOnline( indexDefinition, 10, TimeUnit.SECONDS );
                System.out.printf( "Lucene index online - %s: %s ", label, prop );
                tx.success();
            }
        }
    }

    private void printNodesWithLabel( GraphDatabaseService graphDb, String label )
    {
        ResourceIterator<Node> nodes = graphDb.findNodes( DynamicLabel.label( label ) );
        while ( nodes.hasNext() )
        {
            printNode( nodes.next() );
        }
    }


    private void printNode( Node node )
    {
        for ( Label label : node.getLabels() )
        {
            System.out.print( label.name() + " " );
        }
        System.out.println();

        for ( String propKey : node.getPropertyKeys() )
        {
            System.out.print( "    " + propKey + ": " );
            System.out.println( node.getProperty( propKey ) );
        }
        System.out.println();
    }
}
