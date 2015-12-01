package bench;

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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Random;

import org.neo4j.graphdb.Direction;
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
                        new FlaggedOption( "initialentries", JSAP.INTEGER_PARSER, "10000000", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "initialentries", "How many entries should be in index before " +
                                                                     "start clocking." ),
                        new FlaggedOption( "clockedentries", JSAP.INTEGER_PARSER, "100000", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "clockedentries", "How many insertions should be clocked?" ),
                        new FlaggedOption( "pagesize", JSAP.INTEGER_PARSER, "8192", JSAP.NOT_REQUIRED, 'p', "pagesize",
                                "What page size to use in B" ),
                        new FlaggedOption( "output", OutputtargetParser.INSTANCE, "system", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "output", "Name of output file to append result to. " +
                                                             "Default is system." ),
                        new FlaggedOption( "cachecoverage", JSAP.DOUBLE_PARSER, "0.01,0.1,1,10,25,50,100",
                                JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "cachecoverage",
                                "How large percentage of index should at least be covered by cache? 1-1000" )
                                .setList( true ).setListSeparator( ',' ),
                }
        );

        JSAPResult config = jsap.parse( argv );
        if ( jsap.messagePrinted() ) System.exit( 1 );

        int nbrOfEntries = config.getInt( "initialentries" );
        int nbrOfInserts = config.getInt( "clockedentries" );
        int pageSize = config.getInt( "pagesize" );
        PrintStream output = (PrintStream) config.getObject( "output" );
        double[] cacheCoverageArray = config.getDoubleArray( "cachecoverage" );

        // Calc stuff all in bytes
        int sizeOfLeafEntry = 32;
        int sizeOfInternalEntry = 24;
        int headSize = 21;
        int atLeastNumberOfPages = calcNbrOfPagesDense( nbrOfEntries, pageSize, sizeOfLeafEntry, sizeOfInternalEntry,
                headSize );
        int atMostNumberOfPages = calcNbrOfPagesSparse( nbrOfEntries, pageSize, sizeOfLeafEntry, sizeOfInternalEntry,
                headSize );

        for ( double cacheCoverage : cacheCoverageArray )
        {
            System.out.print( "Inserting with cache coverage " + cacheCoverage + "%\n" );
            System.out.print( "Inserting... " );
            int cachePages = (int) ( cacheCoverage * ( atMostNumberOfPages + atLeastNumberOfPages ) / 2 / 100 );
            int cacheSize = cachePages * pageSize;

            // Practical stuff
            File indexFile = File.createTempFile( SCIndex.filePrefix, SCIndex.indexFileSuffix );
            File metaFile = File.createTempFile( SCIndex.filePrefix, SCIndex.metaFileSuffix );
            SCIndexDescription desc = new SCIndexDescription( "A", "B", "REL", Direction.OUTGOING, null, "property" );

            PageSwapperFactory swapper = new SingleFilePageSwapperFactory();
            swapper.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
            PageCacheTracer tracer = new DefaultPageCacheTracer();
            DefaultPageCacheTracer.enablePinUnpinTracing();
            PageCache pageCache = new MuninnPageCache( swapper, cachePages, pageSize, tracer );

            SCIndex index = new Index( pageCache, indexFile, metaFile, desc, pageSize );

            Random rnd = new Random();
            // Build base index
            for ( int i = 0; i < nbrOfEntries; i++ )
            {
                index.insert( new long[]{rnd.nextLong(), rnd.nextLong()}, new long[]{rnd.nextLong(), rnd.nextLong()} );
            }

            IndexBuildLogger logger = new IndexBuildLogger.HistogramIndexBuildLogger();
            logger.setConfig( cacheSize, pageSize, cachePages, atLeastNumberOfPages, atMostNumberOfPages,
                    cacheCoverage );

            long pins = tracer.countPins();
            long faults = tracer.countFaults();
            // Measure 10000 inserts
            for ( int i = 0; i < nbrOfInserts; i++ )
            {
                logger.startInsert();
                index.insert( new long[]{rnd.nextLong(), rnd.nextLong()}, new long[]{rnd.nextLong(), rnd.nextLong()} );
                logger.finishInsert();
            }

            System.out.print( "OK\n");

            String report = logger.report( tracer.countPins() - pins, tracer.countFaults() - faults );
            output.print( report + "\n" );
        }

        output.close();
    }

    public int calcNbrOfPagesDense( int numberOfEntries, int pageSize, int leafEntrySize, int internalEntrySize,
            int headerSize )
    {
        int nbrOfLeaves = ( numberOfEntries * leafEntrySize + ( pageSize - headerSize ) ) / ( pageSize - headerSize );
        return nbrOfLeaves + recursiceCalcNbrOfInternalNodesAboveThisLevel( nbrOfLeaves,
                (pageSize - headerSize) / internalEntrySize );
    }

    public int calcNbrOfPagesSparse( int numberOfEntries, int pageSize, int leafEntrySize, int internalEntrySize,
            int headerSize )
    {
        int nbrOfLeaves = ( numberOfEntries * leafEntrySize * 2 + ( pageSize - headerSize ) ) / ( pageSize - headerSize );
        return nbrOfLeaves + recursiceCalcNbrOfInternalNodesAboveThisLevel( nbrOfLeaves,
                (pageSize - headerSize) / (2 * internalEntrySize) );
    }

    private int recursiceCalcNbrOfInternalNodesAboveThisLevel( int nbrOfPagesOnLevelBelow, int internalEntriesPerPage )
    {
        if ( nbrOfPagesOnLevelBelow <= 1 )
        {
            return 0;
        }
        else
        {
            int onThisLevel = nbrOfPagesOnLevelBelow / (internalEntriesPerPage + 1);
            return onThisLevel + recursiceCalcNbrOfInternalNodesAboveThisLevel( onThisLevel, internalEntriesPerPage );
        }
    }

    public static void main( String[] argv ) throws JSAPException, IOException
    {
        new BenchmarkBuildTime().run( argv );
    }
}
