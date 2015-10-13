package bench;

import bench.queries.impl.Query1Kernel;
import bench.queries.impl.Query1Shortcut;
import bench.queries.impl.Query2Kernel;
import bench.queries.impl.Query2Shortcut;
import bench.queries.impl.Query3Kernel;
import bench.queries.impl.Query3Shortcut;
import bench.queries.impl.Query4Kernel;
import bench.queries.impl.Query4Shortcut;
import bench.queries.Query;
import bench.queries.impl.Query5Kernel;
import bench.queries.impl.Query5Shortcut;
import bench.queries.impl.Query6Kernel;
import bench.queries.impl.Query6Shortcut;
import bench.util.Config;
import bench.util.GraphDatabaseProvider;
import bench.util.InputDataLoader;
import bench.util.LogSimple;
import index.SCIndexDescription;
import index.ShortcutIndexProvider;
import index.legacy.LegacySCIndex;
import index.SCIndex;

import java.io.FileNotFoundException;
import java.io.IOException;
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
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.tooling.GlobalGraphOperations;

import static bench.util.Config.GRAPH_DB_FOLDER;

public class BenchmarkMain
{

    public static void main( String[] argv ) throws IOException, EntityNotFoundException
    {
        new BenchmarkMain().run( argv );
    }

    private void run( String[] argv ) throws IOException, EntityNotFoundException
    {
        String dbName = Config.LDBC_SF001;

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

    private void benchRun( GraphDatabaseService graphDb ) throws IOException, EntityNotFoundException
    {
        int order = 64;
        ShortcutIndexProvider indexes = new ShortcutIndexProvider();

        // Index for query 1 and 2
        addIndexForQuery( Query1Shortcut.indexDescription, graphDb, order, indexes );
        addIndexForQuery( Query3Shortcut.indexDescription, graphDb, order, indexes );
        addIndexForQuery( Query4Shortcut.indexDescription, graphDb, order, indexes );
        addIndexForQuery( Query5Shortcut.indexDescription, graphDb, order, indexes );
        addIndexForQuery( Query6Shortcut.indexDescription, graphDb, order, indexes );



        Query[] kernelQueries = new Query[]{
                new Query1Kernel(),
                new Query2Kernel(),
                new Query3Kernel(),
                new Query4Kernel(),
                new Query5Kernel(),
                new Query6Kernel(),
        };

        Query[] shortcutQueries = new Query[]{
                new Query1Shortcut( indexes ),
                new Query2Shortcut( indexes ),
                new Query3Shortcut( indexes ),
                new Query4Shortcut( indexes ),
                new Query5Shortcut( indexes ),
                new Query6Shortcut( indexes ),
        };

        // Logger
        BenchLogger logger = new BenchLogger( System.out );

        // --- WITH KERNEL ---
        for ( Query query : kernelQueries )
        {
            benchmarkQuery( query, logger, graphDb, query.inputFile() );
        }

        // --- WITH SHORTCUT ---
        for ( Query query : shortcutQueries )
        {
            benchmarkQuery( query, logger, graphDb, query.inputFile() );
        }

        logger.report( new LogSimple() );
    }

    private void addIndexForQuery( SCIndexDescription description, GraphDatabaseService graphDb, int order,
            ShortcutIndexProvider indexes ) throws IOException
    {
        LegacySCIndex index = new LegacySCIndex( order, description );
        populateShortcutIndex( graphDb, index, description );
        indexes.put( index );
    }

    private void benchmarkQuery(
            Query query, BenchLogger logger, GraphDatabaseService graphDb, String dataFileName )
            throws FileNotFoundException, EntityNotFoundException
    {
        // Load input data
        InputDataLoader inputDataLoader = new InputDataLoader();
        List<long[]> inputData = inputDataLoader.load( dataFileName, query.inputDataHeader() );
        if ( inputData == null )
        {
            Measurement measurement = logger.startQuery( query.queryDescription(), query.type() );
            measurement.error( "Failed to load input data" );
        }
        else
        {

            // Get context bridge
            ThreadToStatementContextBridge threadToStatementContextBridge =
                    ((GraphDatabaseAPI) graphDb).getDependencyResolver()
                            .resolveDependency( ThreadToStatementContextBridge.class );

            // Start logging
            Measurement measurement = logger.startQuery( query.queryDescription(), query.type() );

            // Start clock
            long start = System.nanoTime();
            boolean first = true;
            // Run query
            for ( long[] input : inputData )
            {
                query.runQuery( threadToStatementContextBridge, graphDb, measurement, input );
                if ( first )
                {
                    measurement.firstQueryFinished( (System.nanoTime() - start) / 1000 );
                    first = false;
                }
            }
            measurement.lastQueryFinished( (System.nanoTime() - start) / 1000 );

        }
    }

    private void populateShortcutIndex( GraphDatabaseService graphDb, SCIndex index,
            SCIndexDescription desc ) throws IOException
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


    // TODO: Fix this to populate all indexes at once
    private void populateShortcutIndex( GraphDatabaseService graphDb, SCIndex index, String firstLabelName,
            String relTypeName, Direction dir, String secondLabelName, String propName, boolean propOnRel )
            throws IOException
    {
        System.out.println( "INDEX PATTERN: " + index.getDescription() );
        System.out.print( "Building... " );
        int numberOfInsert = 0;
        try ( Transaction tx = graphDb.beginTx() )
        {
            Label firstLabel = DynamicLabel.label( firstLabelName );
            Label secondLabel = DynamicLabel.label( secondLabelName );

            for ( Relationship rel : GlobalGraphOperations.at( graphDb ).getAllRelationships() )
            {
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
                        long prop = propOnRel ? ((Number) rel.getProperty( propName )).longValue() :
                                    ((Number) second.getProperty( propName )).longValue();
                        index.insert( new long[]{first.getId(), prop }, new long[]{ rel.getId(), second.getId() } );
                    }
                }
            }
            tx.success();
        }
        System.out.printf( "OK [index size %d]\n", numberOfInsert );
    }

    @SuppressWarnings( "unused" )
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
