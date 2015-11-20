package bench;

import bench.queries.Query;
import bench.util.Dataset;
import bench.util.GraphDatabaseProvider;
import bench.util.IndexLoader;
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
import index.SCIndexProvider;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
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
                        new FlaggedOption( "warmup", JSAP.INTEGER_PARSER, "1", JSAP.NOT_REQUIRED, 'w', "warmup",
                                "Number of warm up iterations"),
                        new FlaggedOption( "inputsize", JSAP.INTEGER_PARSER, "10000", JSAP.NOT_REQUIRED, 's', "inputsize",
                                "Max number of different input data per query, " +
                                "decides how many time each query is run in every iteration." ),
                        new FlaggedOption( "pagesize", JSAP.INTEGER_PARSER, "8192", JSAP.NOT_REQUIRED, 'p', "pagesize",
                                "What page size to use in B" ),
                        new FlaggedOption( "cachesize", JSAP.INTEGER_PARSER, "1024", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "cachesize", "Max size of index cache in MB" ),
                        new FlaggedOption( "dataset", DatasetParser.INSTANCE, "ldbc1", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "dataset",
                                "Decide what dataset to use. ldbc1, ldbc10, lab8 40 200 400 800 1600" ),
                        new FlaggedOption( "workload", WorkloadParser.INSTANCE, "ldbcall", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "workload",
                                "What workload to use. Environment need to match dataset. " +
                                "<ldbcall | ldbcholy | laball | ldbc1 | ldbc 2 | ldbc3 | ldbc4 | ldbc5 | ldbc6 | " +
                                "lab100 | lab75 | lab50 | lab 25 | lab1 | lablimit>" ),
                        new FlaggedOption( "output", OutputtargetParser.INSTANCE, "system", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "output", "Name of output file to append result to. " +
                                                             "Default is system." ),
                        new Switch( "forceNew", JSAP.NO_SHORTFLAG, "force", "Switch to force creation of new index." ),
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

        // Make sure entire workload fits dataset
        for ( Query query : workload.queries() )
        {
            if ( query.environment() != dataset.environment )
            {
                throw new IllegalStateException( "Environment for " + query.queryDescription().queryName() +
                " does not match environment of dataset " + dataset.dbName );
            }
        }

        // Setup run configurations
        BenchConfig benchConfig = new BenchConfig( pageSize, cachePages, inputSize, nbrOfWarmup, dataset.inputDataDir );

        // Open database
        GraphDatabaseService graphDb = GraphDatabaseProvider.openDatabase( dataset.dbPath, dataset.dbName );

        // Initiate page cache
        PageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        swapper.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        PageCacheTracer tracer = new DefaultPageCacheTracer();
        PageCache pageCache = new MuninnPageCache( swapper, cachePages, pageSize, tracer );

        // Load indexes
        String indexPath = dataset.dbPath + dataset.dbName + "/index/";
        SCIndexProvider provider = IndexLoader.loadIndexes( graphDb, pageCache, indexPath, benchConfig, workload );

        // Give index provider to shortcut queries
        List<Query> queries = workload.queries();
        for ( Query query : queries )
        {
            query.setIndexProvider( provider );
        }

        // Load input data
        Map<String,List<long[]>> inputData = loadInputData( benchConfig, queries );

        // Run workload
        benchRun( graphDb, strategy, queries, benchConfig, dataset, inputData, output );

        // Close
        provider.close();
    }

    private void benchRun( GraphDatabaseService graphDb, LogStrategy logStrategy, List<Query> queries,
            BenchConfig benchConfig, Dataset dataset, Map<String,List<long[]>> inputData, PrintStream output )
            throws IOException, EntityNotFoundException
    {
        // Get context bridge
        ThreadToStatementContextBridge threadToStatementContextBridge =
                ((GraphDatabaseAPI) graphDb).getDependencyResolver()
                        .resolveDependency( ThreadToStatementContextBridge.class );

        // Logger
        Logger liveLogger = new BenchLogger( output, benchConfig, dataset );
        Logger warmUpLogger = Logger.DUMMY_LOGGER;

        // Pause to start recorder

        // --- RUN QUERIES ---
        benchmarkQueriesWithWarmUp( queries, warmUpLogger, liveLogger, graphDb, threadToStatementContextBridge,
                inputData, benchConfig.numberOfWarmups() );

        liveLogger.report( logStrategy );
        liveLogger.close();
    }


    private void benchmarkQueriesWithWarmUp( List<Query> queries, Logger warmupLogger, Logger liveLogger,
            GraphDatabaseService graphDb, ThreadToStatementContextBridge threadToStatementContextBridge,
            Map<String,List<long[]>> inputData, int nbrOfWarmup )
            throws IOException, EntityNotFoundException
    {
        System.out.println( "Starting warmup." );
        // Warm up
        for ( int i = 0; i < nbrOfWarmup; i++ )
        {
            System.out.print( "Warmup iteration " + (i+1) + "... " );
            for ( Query query : queries )
            {
                benchmarkQuery( query, warmupLogger, graphDb, threadToStatementContextBridge,
                        inputData.get( query.inputFile() ) );
            }
            System.out.println( "ok");
        }
        System.out.println( "Warmup finished. Did " + nbrOfWarmup + " run(s).\nStarting live run." );
        // Live
        for ( Query query : queries )
        {
            benchmarkQuery( query, liveLogger, graphDb, threadToStatementContextBridge,
                    inputData.get( query.inputFile() ) );
        }
        System.out.println( "Live run finished." );
    }

    private void benchmarkQuery( Query query, Logger logger, GraphDatabaseService graphDb,
            ThreadToStatementContextBridge threadToStatementContextBridge, List<long[]> inputData )
            throws IOException, EntityNotFoundException
    {
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

    private Map<String,List<long[]>> loadInputData( BenchConfig benchConfig, List<Query> queries )
            throws FileNotFoundException
    {
        Map<String,List<long[]>> inputData = new HashMap<>();
        InputDataLoader inputDataLoader = new InputDataLoader();

        for ( Query query : queries )
        {
            if ( !inputData.containsKey( query.inputFile() ) )
            {
                List<long[]> data = inputDataLoader.load( benchConfig.inputDataDir(), query.inputFile(), query.inputDataHeader(),
                        benchConfig.inputSize() );
                if ( data == null )
                {
                    throw new RuntimeException( "Failed to load input data for query " + query.cypher() +
                                                " using file " + query.inputFile() );
                }
                inputData.put( query.inputFile(), data );
            }
        }

        return inputData;
    }

    @SuppressWarnings( "unused" )
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
