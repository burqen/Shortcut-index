package bench;

import bench.queries.Query;
import bench.util.Dataset;
import bench.util.GraphDatabaseProvider;
import bench.util.InputDataLoader;
import bench.util.Workload;
import bench.util.arguments.DatasetParser;
import bench.util.arguments.LoggerParser;

import bench.util.arguments.OutputtargetParser;
import bench.util.arguments.WorkloadParser;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

import com.martiansoftware.jsap.Switch;
import com.sun.deploy.config.Config;
import index.SCIndexDescription;
import index.SCIndexProvider;
import index.btree.Index;
import index.SCIndex;
import index.storage.ByteArrayPagedFile;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.tooling.GlobalGraphOperations;

public class BenchmarkMain
{
    public static void main( String[] argv ) throws IOException, EntityNotFoundException, JSAPException
    {
        new BenchmarkMain().run( argv );
    }

    private void run( String[] argv ) throws IOException, EntityNotFoundException, JSAPException
    {
        SimpleJSAP jsap = new SimpleJSAP(
                "BenchmarkMain",
                "Run benchmarks on selected queries",
                new Parameter[] {
                        new FlaggedOption( "logger", LoggerParser.INSTANCE,
                                "simple", JSAP.NOT_REQUIRED, 'l', "logger", "Decide which logger to use." )
                                .setList( false )
                                .setHelp( "Decide which logger to use: simple, simpletime, latex, histo or histotime" ),
                        new FlaggedOption( "warmup", JSAP.INTEGER_PARSER, "10", JSAP.NOT_REQUIRED, 'w', "warmup",
                                "Number of warm up iterations"),
                        new FlaggedOption( "inputsize", JSAP.INTEGER_PARSER, "10000", JSAP.NOT_REQUIRED, 's', "inputsize",
                                "Max number of different input data per query, " +
                                "decides how many time each query is run in every iteration." ),
                        new FlaggedOption( "pagesize", JSAP.INTEGER_PARSER, "8192", JSAP.NOT_REQUIRED, 'p', "pagesize",
                                "What page size to use in B" ),
                        new FlaggedOption( "cachesize", JSAP.INTEGER_PARSER, "2048", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "cachesize", "Max size of index cache in MB" ),
                        new FlaggedOption( "dataset", DatasetParser.INSTANCE, "ldbc1", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "dataset",
                                "Decide what dataset to use. ldbc1, lab8 40 200 400 800" ),
                        new FlaggedOption( "workload", WorkloadParser.INSTANCE, "ldbcall", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "workload",
                                "What workload to use. Environment need to match dataset. " +
                                "<ldbcall | laball | ldbc1 | ldbc 2 | ldbc3 | ldbc4 | ldbc5 | ldbc6 | " +
                                "lab100 | lab75 | lab50 | lab 25 | lab1>" ),
                        new FlaggedOption( "output", OutputtargetParser.INSTANCE, "system", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "output", "Name of output file to append result to. " +
                                                             "Default is system." ),
                        new Switch( "forceNew", JSAP.NO_SHORTFLAG, "force", "Switch to force creation of new index." )
                }
        );

        JSAPResult config = jsap.parse(argv);
        if ( jsap.messagePrinted() ) System.exit( 1 );

        LogStrategy strategy = (LogStrategy) config.getObject( "logger" );
        int nbrOfWarmup = config.getInt( "warmup" );
        int inputSize = config.getInt( "inputsize" );
        int pageSize = config.getInt( "pagesize" );
        int cachePages = config.getInt( "cachesize" ) * 1000000 / pageSize;
        Dataset dataset = (Dataset) config.getObject( "dataset" );
        Workload workload = (Workload) config.getObject( "workload" );
        PrintStream output = (PrintStream) config.getObject( "output" );
        boolean forceNew = Config.getBooleanProperty( "forceNew" );

        // Make sure entire workload fits dataset
        for ( Query query : workload.queries() )
        {
            if ( query.environment() != dataset.environment )
            {
                throw new IllegalStateException( "Environment for " + query.queryDescription().queryName() +
                " does not match environment of dataset " + dataset.dbName );
            }
        }

        BenchConfig benchConfig = new BenchConfig( pageSize, cachePages, inputSize, nbrOfWarmup );

        GraphDatabaseService graphDb = GraphDatabaseProvider.openDatabase( dataset.dbPath, dataset.dbName );

        // TODO: Should hold all indexes so that we can close them. Don't only give them to queries.
        List<Query> queries = loadIndexes( graphDb, dataset, benchConfig, workload, forceNew );

        benchRun( graphDb, strategy, queries, benchConfig, dataset, output );
    }

    private void benchRun( GraphDatabaseService graphDb, LogStrategy logStrategy, List<Query> queries,
            BenchConfig benchConfig, Dataset dataset, PrintStream output )
            throws IOException, EntityNotFoundException
    {
        // Input data
        Map<String,List<long[]>> inputData = new HashMap<>();
        InputDataLoader inputDataLoader = new InputDataLoader();
        for ( Query query : queries )
        {
            if ( !inputData.containsKey( query.inputFile() ) )
            {
                List<long[]> data = inputDataLoader.load( query.inputFile(), query.inputDataHeader(),
                        benchConfig.inputSize() );
                if ( data == null )
                {
                    throw new RuntimeException( "Failed to load input data for query " + query.cypher() +
                                                " using file " + query.inputFile() );
                }
                inputData.put( query.inputFile(), data );
            }
        }

        // Logger
        Logger liveLogger = new BenchLogger( output, benchConfig, dataset );
        Logger warmUpLogger = Logger.DUMMY_LOGGER;

        // Pause to start recorder

        // --- RUN QUERIES ---
        benchmarkQueriesWithWarmUp( queries, warmUpLogger, liveLogger, graphDb, inputData,
                benchConfig.numberOfWarmups() );

        liveLogger.report( logStrategy );
        liveLogger.close();
    }

    private List<Query> loadIndexes( GraphDatabaseService graphDb, Dataset dataset, BenchConfig benchConfig,
            Workload workload, boolean forceNew )
            throws IOException
    {
        SCIndexProvider provider = new SCIndexProvider();

        // Populate provider
        Iterator<SCIndexDescription> indexDescriptions = workload.indexDescriptions();
        while ( indexDescriptions.hasNext() )
        {
            SCIndexDescription description = indexDescriptions.next();
            addIndexForQuery( description, graphDb, benchConfig.pageSize(), benchConfig.cachePages(), provider );
        }

        // Give provider to shortcut queries
        List<Query> queries = workload.queries();
        for ( Query query : queries )
        {
            query.setIndexProvider( provider );
        }
        return queries;
    }

    // Can be used to pause execution and turn on flight recorder
    private void pause( int seconds )
    {
        System.out.println( "Resuming operation in..." );
        long currentTime = System.currentTimeMillis();
        long endTime = currentTime + seconds * 1000; // 10 sec from now
        int countDown = seconds;
        while ( currentTime < endTime )
        {
            System.out.println( countDown-- );
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException e )
            {
                e.printStackTrace();
            }
            currentTime = System.currentTimeMillis();
        }
    }

    private void benchmarkQueriesWithWarmUp( List<Query> queries, Logger warmupLogger, Logger liveLogger,
            GraphDatabaseService graphDb, Map<String,List<long[]>> inputData, int nbrOfWarmup )
            throws IOException, EntityNotFoundException
    {
        // Warm up
        for ( int i = 0; i < nbrOfWarmup; i++ )
        {
            for ( Query query : queries )
            {
                benchmarkQuery( query, warmupLogger, graphDb, inputData.get( query.inputFile() ) );
            }
        }
        // Live
        for ( Query query : queries )
        {
            benchmarkQuery( query, liveLogger, graphDb, inputData.get( query.inputFile() ) );
        }
    }

    private void benchmarkQuery(
            Query query, Logger logger, GraphDatabaseService graphDb, List<long[]> inputData )
            throws IOException, EntityNotFoundException
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

    private void addIndexForQuery( SCIndexDescription description, GraphDatabaseService graphDb, int pageSize,
            int cachePages, SCIndexProvider indexes ) throws IOException
    {
        String prefix = SCIndex.filePrefix;
        String suffix = SCIndex.indexFileSuffix;
        String metaSuffix = SCIndex.metaFileSuffix;
        File indexFile = File.createTempFile( prefix, suffix );
        File metaFile = File.createTempFile( prefix, metaSuffix );

        PageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        swapper.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        PageCacheTracer tracer = new DefaultPageCacheTracer();

        PageCache muninnPageCache = new MuninnPageCache( swapper, cachePages, pageSize, tracer );

        SCIndex index = new Index( muninnPageCache, indexFile, metaFile, description, pageSize );

        populateShortcutIndex( graphDb, index, description );
        indexes.put( index );
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
}
