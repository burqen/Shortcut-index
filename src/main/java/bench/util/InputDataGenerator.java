package bench.util;

import bench.queries.Query;
import bench.util.arguments.DatasetParser;
import bench.util.arguments.WorkloadParser;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

public class InputDataGenerator
{

    public void run( String[] argv ) throws IOException, JSAPException
    {
        SimpleJSAP jsap = new SimpleJSAP(
                "InputDataGenerator",
                "Generate data for selected queries",
                new Parameter[] {
                        new FlaggedOption( "dataset", DatasetParser.INSTANCE, "ldbc1", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "dataset",
                                "Decide what dataset to use. ldbc1, ldbc10, lab8 40 200 400 800 1600" ),
                        new FlaggedOption( "workload", WorkloadParser.INSTANCE, "ldbcall", JSAP.NOT_REQUIRED,
                                JSAP.NO_SHORTFLAG, "workload",
                                "What workload to use. Environment need to match dataset. " +
                                "<ldbcall | ldbcholy | laball | ldbc1 | ldbc 2 | ldbc3 | ldbc4 | ldbc5 | ldbc6 | " +
                                "lab100 | lab75 | lab50 | lab 25 | lab1>" ),
                }
        );

        JSAPResult config = jsap.parse(argv);
        if ( jsap.messagePrinted() ) System.exit( 1 );

        Dataset dataset = (Dataset) config.getObject( "dataset" );
        Workload workload = (Workload) config.getObject( "workload" );

        // Make sure entire workload fits dataset
        for ( Query query : workload.queries() )
        {
            if ( query.environment() != dataset.environment )
            {
                throw new IllegalStateException( "Environment for " + query.queryDescription().queryName() +
                                                 " does not match environment of dataset " + dataset.dbName );
            }
        }

        if ( argv.length == 0 )
        {
            System.out.println( "Expected input of combination of query2, query4, query5, query6" );
            System.exit( 1 );
        }

        String dbName = dataset.dbName;

        GraphDatabaseService graphDb = GraphDatabaseProvider.openDatabase( Config.GRAPH_DB_FOLDER, dbName );

        workload.queries();
        for ( Query query : workload.queries() )
        {
            generateInputForQuery( graphDb, dataset.inputDataDir, query );
        }
    }

    private void generateInputForQuery( GraphDatabaseService graphDb, String inputDataDir, Query query ) throws IOException
    {
        if ( query.inputFile() == Config.NO_INPUT )
        {
            return;
        }

        String file = inputDataDir + query.inputFile();
        System.out.println( "Create input data for query with input header "
                            + Arrays.toString( query.inputDataHeader() ) );
        System.out.print( "Open file " + file + "... " );
        FileOutputStream os = openOutputStream( file );
        if ( os == null )
        {
            return;
        }
        PrintWriter out = new PrintWriter( os );
        System.out.print( "ok\n");
        System.out.print( "Generate random ids... " );
        List<Long> randomIds = generateRandomIdsForLabel( graphDb, query.inputDataHeader()[0] );
        System.out.print( "ok\n" );

        char separator = InputDataLoader.csvSeparator;

        // HEADER
        String[] header = query.inputDataHeader();
        printHeader( out, header, separator );

        // ... and shuffle the order and write to file...
        // CONTENT
        Collections.shuffle( randomIds );
        System.out.print( "Writing to file..." );
        randomIds.forEach( out::println );
        System.out.print( "ok\n" );

        out.close();
        System.out.println( "SUCCESS" );
    }

    private List<Long> generateRandomIdsForLabel( GraphDatabaseService graphDb, String label )
    {
        // Count the number of nodes with label Person...
        int numberOfPersons = 0;
        try ( Transaction tx = graphDb.beginTx() )
        {
            ReadOperations operations = ((GraphDatabaseAPI) graphDb).getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class )
                    .get().readOperations();

            int labelId = operations.labelGetForName( label );
            PrimitiveLongIterator nodesWithLabel = operations.nodesGetForLabel( labelId );

            while ( nodesWithLabel.hasNext() )
            {
                nodesWithLabel.next();
                numberOfPersons++;
            }

            tx.success();
        }

        int desired = Config.MAX_NUMBER_OF_QUERY_REPETITIONS;
        Random rng = new Random();
        Set<Integer> nodesToAddFromOrderedListOfNodes = new TreeSet<>();

        // ... to see if there are enough ...
        if ( numberOfPersons <= desired )
        {
            // ... if not just pick all of them ...
            for ( int i = 0; i < numberOfPersons; i++ )
            {
                nodesToAddFromOrderedListOfNodes.add( i );
            }
        }
        else
        {
            // ... or if so, randomly generate what persons to pick out of an ordered list ...
            while ( nodesToAddFromOrderedListOfNodes.size() < desired )
            {
                nodesToAddFromOrderedListOfNodes.add( rng.nextInt( numberOfPersons ) );
            }
        }

        // ... then walk through that ordered list which turns out to be an iterator, and pick out the
        // persons decided upon...
        List<Long> nodeIds = new ArrayList<>();
        try ( Transaction tx = graphDb.beginTx() )
        {
            ReadOperations operations = ((GraphDatabaseAPI) graphDb).getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class )
                    .get().readOperations();

            int labelId = operations.labelGetForName( label );
            PrimitiveLongIterator nodesWithLabel = operations.nodesGetForLabel( labelId );

            int count = 0;
            Iterator<Integer> nodesToAddIt = nodesToAddFromOrderedListOfNodes.iterator();
            int nextToAdd = nodesToAddIt.next();
            while ( nodesWithLabel.hasNext() )
            {
                long nextId = nodesWithLabel.next();
                if ( count == nextToAdd )
                {
                    nodeIds.add( nextId );
                    if ( !nodesToAddIt.hasNext() )
                    {
                        break;
                    }
                    nextToAdd = nodesToAddIt.next();
                }
                count++;
            }

            tx.success();
        }
        // ... and lastly think about if this is the easiest way to do this. Probably not.
        return nodeIds;
    }

    private void printHeader( PrintWriter out, String[] header, char separator )
    {
        out.print( header[0] );
        for ( int i = 1; i < header.length; i++ )
        {
            out.print( separator );
            out.print( header[i] );
        }
        out.print( "\n" );
    }

    public FileOutputStream openOutputStream( String filePath )
            throws IOException
    {
        File file;
        FileOutputStream out;

        file = new File( filePath );

        // if file doesnt exists, then create it
        if ( !file.exists() )
        {
            file.createNewFile();
        }
        else
        {
            System.out.println( "File " + filePath + " already exist. Please delete it before generating a new.");
            return null;
        }

        out = new FileOutputStream( file );

        return out;
    }

    public static void main( String[] argv ) throws IOException, JSAPException
    {
        new InputDataGenerator().run( argv );
    }
}
