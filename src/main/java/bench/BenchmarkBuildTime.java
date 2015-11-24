package bench;

import bench.util.Dataset;
import bench.util.GraphDatabaseProvider;
import bench.util.IndexLoader;
import bench.util.LogCompleteHistogram;
import bench.util.arguments.DatasetParser;
import bench.util.arguments.IndexDescriptionParser;
import bench.util.arguments.OutputtargetParser;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import index.SCIndex;
import index.SCIndexDescription;
import index.btree.Index;
import org.HdrHistogram.Histogram;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

public class BenchmarkBuildTime
{
    public void run( String[] argv ) throws JSAPException, IOException
    {
        SimpleJSAP jsap = new SimpleJSAP(
                "BenchmarkBuildTime",
                "Build index of on selected dataset and measure response time",
                new Parameter[] {
                        new FlaggedOption( "pagesize", JSAP.INTEGER_PARSER, "8192", JSAP.NOT_REQUIRED, 'p', "pagesize",
                                "What page size to use in B" ),
                        new FlaggedOption( "cachesize", JSAP.INTEGER_PARSER, "1024", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "cachesize", "Max size of index cache in MB" ),
                        new FlaggedOption( "dataset", DatasetParser.INSTANCE, "lab8", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "dataset",
                                "Decide what dataset to use. ldbc1, ldbc10, lab8 40 200 400 800 1600" ),
                        new FlaggedOption( "output", OutputtargetParser.INSTANCE, "system", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "output", "Name of output file to append result to. " +
                                                             "Default is system." ),
                        new FlaggedOption( "workload", IndexDescriptionParser.INSTANCE, "lab", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "workload",
                                "What workload to use. Environment need to match dataset. " +
                                "<lab>" ),
                }
        );

        JSAPResult config = jsap.parse( argv );
        if ( jsap.messagePrinted() ) System.exit( 1 );

        int pageSize = config.getInt( "pagesize" );
        int cachePages = config.getInt( "cachesize" ) * 1000000 / pageSize;
        Dataset dataset = (Dataset) config.getObject( "dataset" );
        SCIndexDescription desc = (SCIndexDescription) config.getObject( "workload" );
        PrintStream output = (PrintStream) config.getObject( "output" );

        GraphDatabaseService graphDb = GraphDatabaseProvider.openDatabase( dataset.dbPath, dataset.dbName );

        // Initiate page cache
        buildIndexFromDescription( pageSize, cachePages, graphDb, desc, output );
    }

    private void buildIndexFromDescription( int pageSize, int cachePages, GraphDatabaseService graphDb,
            SCIndexDescription desc, PrintStream output ) throws IOException
    {
        PageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        swapper.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        PageCacheTracer tracer = new DefaultPageCacheTracer();
        PageCache pageCache = new MuninnPageCache( swapper, cachePages, pageSize, tracer );

        File indexFile = File.createTempFile( SCIndex.filePrefix, SCIndex.indexFileSuffix );
        File metaFile = File.createTempFile( SCIndex.filePrefix, SCIndex.metaFileSuffix );
        SCIndex index = new Index( pageCache, indexFile, metaFile, desc, pageSize );

        Histogram histogram = new Histogram(
                TimeUnit.MICROSECONDS.convert( 1, TimeUnit.MINUTES ), 5 );
        IndexBuildLogger logger = new IndexBuildLogger.HistogramIndexBuildLogger( histogram );

        IndexLoader.populateShortcutIndex( graphDb, index, index.getDescription(), logger );

        // Print histogram
        String histogramString = LogCompleteHistogram.staticHistogramString( histogram, "Insert time" );
        output.print( histogramString + "\n" );
    }

    public static void main( String[] argv ) throws JSAPException, IOException
    {
        new BenchmarkBuildTime().run( argv );
    }
}
