package bench;

import au.com.bytecode.opencsv.CSVReader;
import bench.queries.Measurement;
import bench.queries.QueryX;
import bench.queries.QueryXKernel;
import bench.queries.QueryXShortcut;
import index.logical.ShortcutIndexDescription;
import index.logical.ShortcutIndexProvider;
import index.logical.ShortcutIndexService;
import index.logical.TKey;
import index.logical.TValue;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.tooling.GlobalGraphOperations;

public class BenchmarkMain
{
    private static String NO_INPUT = "";
    private char csvSeparator = ',';
    private String resourcePath = "src/main/resources/";
    private String dbPath = "ldbc_sf001_p006_Neo4jDb";

    public static void main( String[] argv )
    {
        new BenchmarkMain().run( argv );
    }

    private void run( String[] argv )
    {

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

    private void benchRun( GraphDatabaseService graphDb )
    {
        int order = 64;
        ShortcutIndexDescription description = QueryX.indexDescription;
        ShortcutIndexService index = new ShortcutIndexService( order, description );

        populateShortcutIndex( graphDb, index,
                "Person", "COMMENT_HAS_CREATOR", Direction.INCOMING, "Comment", "creationDate", false );

        ShortcutIndexProvider indexes = new ShortcutIndexProvider();
        indexes.put( index );

        initiateLuceneIndex( graphDb, "Person", "firstName" );

        // Logger
        BenchLogger logger = new BenchLogger( System.out );

        // *** QUERY X ***
        // --- WITH KERNEL ---
        QueryXKernel kernelQuery = new QueryXKernel();
        benchmarkQuery( kernelQuery, logger, graphDb, NO_INPUT );
        logger.report();

        // --- WITH SHORTCUT ---
        QueryXShortcut shortcutQuery = new QueryXShortcut( indexes );
        benchmarkQuery( shortcutQuery, logger, graphDb, NO_INPUT );
        logger.report();
    }

    private void benchmarkQuery( BaseQuery query, BenchLogger logger, GraphDatabaseService graphDb, String dataFileName )
    {
        // Load input data
        List<long[]> inputData = loadInputData( dataFileName, query.inputDataHeader() );

        // Start logging
        Measurement measurement = logger.startQuery( query.query() );

        // Run query
        for ( long[] input : inputData )
        {
            query.runQuery( graphDb, measurement, input );
        }
        measurement.close();
    }

    private List<long[]> loadInputData( String dataFileName, String[] expectedHeader )
    {
        List<long[]> inputData = new ArrayList<>();
        if ( !dataFileName.equals( NO_INPUT ) )
        {
            InputStream in = getClass().getResourceAsStream( resourcePath + dataFileName );
            BufferedReader reader = new BufferedReader( new InputStreamReader( in ) );
            String[] line;

            try ( CSVReader csvReader = new CSVReader( reader, csvSeparator ) )
            {
                String[] actualHeader = csvReader.readNext();
                if ( !Arrays.equals( expectedHeader, actualHeader ) )
                {
                    throw new RuntimeException(
                            String.format( "Input has wrong header. Expected header: %s, Actual header%s\n",
                                    expectedHeader, actualHeader ) );
                }

                // Ready to read input
                while ( (line = csvReader.readNext()) != null )
                {
                    long[] input = new long[line.length];
                    for ( int i = 0; i < line.length; i++ )
                    {
                        input[i] = Long.parseLong( line[i] );
                    }
                    inputData.add( input );
                }
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                throw new RuntimeException( e.getCause() );
            }
        }
        else
        {
            inputData.add( BaseQuery.NO_DATA );
        }
        return inputData;
    }

    private void populateShortcutIndex( GraphDatabaseService graphDb, ShortcutIndexService index,
            ShortcutIndexDescription desc )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            Label firstLabel = DynamicLabel.label( desc.firstLabel );
            Label secondLabel = DynamicLabel.label( desc.secondLabel );

            Iterator<Relationship> allRelationships =
                    GlobalGraphOperations.at( graphDb ).getAllRelationships().iterator();
            int numberOfInsert = 0;
            int numberOfRelationships = 0;
            while ( allRelationships.hasNext() )
            {
                Relationship rel = allRelationships.next();
                numberOfRelationships++;
                if ( rel.getType().name().equals( desc.relationshipType ) )
                {
                    Node first;
                    Node second;
                    if ( desc.direction == Direction.OUTGOING )
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
                        long prop = desc.relationshipPropertyKey != null ?
                                    (long) rel.getProperty( desc.relationshipPropertyKey ) :
                                    (long) second.getProperty( desc.nodePropertyKey );
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
