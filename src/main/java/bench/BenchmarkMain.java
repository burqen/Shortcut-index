package bench;

import bench.queries.Query1Kernel;
import bench.queries.Query1Shortcut;
import index.logical.ShortcutIndexDescription;
import index.logical.ShortcutIndexProvider;
import index.logical.ShortcutIndexService;
import index.logical.TKey;
import index.logical.TValue;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.tooling.GlobalGraphOperations;

public class BenchmarkMain
{
    public static void main( String[] argv )
    {
        new BenchmarkMain().run( argv );
    }

    private void run( String[] argv )
    {
        String resourcePath = "src/main/resources/";
        String dbPath = "ldbc_sf001_p006_Neo4jDb";

        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( resourcePath + dbPath )
                .setConfig( "allow_store_upgrade", "true" )
                .newGraphDatabase();
        registerShutdownHook( graphDb );

        if ( argv[0].equals( "bench" ) )
        {
            benchRun( graphDb );
        }
        else if (argv[0].equals( "alt" ) )
        {
            try ( Transaction tx = graphDb.beginTx() )
            {
                ReadOperations readOperations = ((GraphDatabaseAPI) graphDb).getDependencyResolver()
                        .resolveDependency( ThreadToStatementContextBridge.class )
                        .get().readOperations();

                System.out.println( "ALL PROPERTY KEYS" );
                ResourceIterable<String> allPropertyKeys = GlobalGraphOperations.at( graphDb ).getAllPropertyKeys();
                for ( String propKey : allPropertyKeys )
                {
                    System.out.println( propKey );
                }
                System.out.println();

                System.out.println( "ALL RELATIONSHIP TYPES" );
                Iterator<RelationshipType> allTypes =
                        GlobalGraphOperations.at( graphDb ).getAllRelationshipTypes().iterator();
                while ( allTypes.hasNext() )
                {
                    RelationshipType next = allTypes.next();
                    System.out.println( next.name() );
                }
                System.out.println();
//
//                PrimitiveLongIterator persons =
//                        readOperations.nodesGetForLabel( readOperations.labelGetForName( "Comment" ) );
//
//                while ( persons.hasNext() )
//                {
//                    long node = persons.next();
//                    printNode( graphDb.getNodeById( node ) );
//                }
                tx.success();
            }

        }
    }

    private void benchRun( GraphDatabaseService graphDb )
    {
        int order = 2;
        ShortcutIndexDescription description = new ShortcutIndexDescription( "simple" );
        ShortcutIndexService index = new ShortcutIndexService( order, description );

        populateShortcutIndex( graphDb, index,
                "Person", "COMMENT_HAS_CREATOR", Direction.INCOMING, "Comment", "creationDate", false );

        ShortcutIndexProvider indexes = new ShortcutIndexProvider();
        indexes.put( index );

        initiateLuceneIndex( graphDb, "Person", "firstName" );

        // --- QUERY 1 ---
        // --- WITH KERNEL ---
        Query1Kernel kernelQuery = new Query1Kernel();
        BenchLogger logger = new BenchLogger( System.out );
        kernelQuery.runQuery( graphDb, logger );
        logger.report();

        // --- WITH SHORTCUT ---
        Query1Shortcut shortcutQuery = new Query1Shortcut( indexes );
        logger = new BenchLogger( System.out );
        shortcutQuery.runQuery( graphDb, logger );
        logger.report();
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
                        Comparable prop = propOnRel ? (Comparable) rel.getProperty( propName ) :
                                          (Comparable) second.getProperty( propName );
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
            }
        }
    }

    private void printNode( Node node )
    {
        Iterator<Label> labels = node.getLabels().iterator();
        while ( labels.hasNext() )
        {
            Label label = labels.next();
            System.out.print( label.name() + " " );
        }
        System.out.println();

        Iterator<String> propKeys = node.getPropertyKeys().iterator();
        while ( propKeys.hasNext() )
        {
            String propKey = propKeys.next();
            System.out.print( "    " + propKey + ": " );
            System.out.println( node.getProperty( propKey ) );
        }
        System.out.println();
    }

    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
}
