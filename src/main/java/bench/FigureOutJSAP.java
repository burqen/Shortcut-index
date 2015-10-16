package bench;

import bench.util.arguments.LoggerParser;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

import java.io.IOException;

public class FigureOutJSAP
{

    public static void main( String[] argv ) throws JSAPException, IOException
    {
        SimpleJSAP jsap = new SimpleJSAP(
                "BenchmarkMain",
                "Run benchmarks on selected queries",
                new Parameter[] {
                        new FlaggedOption( "logger", LoggerParser.INSTANCE,
                                "simple", JSAP.NOT_REQUIRED, 'l', "logger", "Decide which logger to use." )
                                .setList( false )
                                .setHelp( "Decide which logger to use: simple, latex or histo" ),
                        new FlaggedOption( "warmup", JSAP.INTEGER_PARSER, "10", JSAP.NOT_REQUIRED, 'w', "warmup",
                                "Number of warm up iterations"),
                        new FlaggedOption( "inputsize", JSAP.INTEGER_PARSER, "1000", JSAP.NOT_REQUIRED, 's', "inputsize",
                                "Max number of different input data per query, " +
                                "decides how many time each query is run in every iteration." ),
                        new FlaggedOption( "iteration", JSAP.INTEGER_PARSER, "10", JSAP.NOT_REQUIRED, 'i', "iteration",
                                "Number of iterations to run after warm up." ),
                        new FlaggedOption( "pagesize", JSAP.INTEGER_PARSER, "8192", JSAP.NOT_REQUIRED, 'p', "pagesize",
                                "What page size in bytes to use" )
                }
        );

        JSAPResult config = jsap.parse(argv);
        if ( jsap.messagePrinted() ) System.exit( 1 );

        LogStrategy strategy = (LogStrategy) config.getObject( "logger" );
        int nbrOfWarmup = config.getInt( "warmup" );
        int inputSize = config.getInt( "inputsize" );
        int nbrOfIterations = config.getInt( "iteration" );
        int pageSize = config.getInt( "pagesize" );
    }
}
