package bench.util;

import bench.queries.impl.lab.LabQuery1Kernel;
import bench.queries.impl.ldbc.Query2Kernel;
import bench.queries.impl.ldbc.Query4Kernel;
import bench.queries.Query;
import bench.queries.impl.ldbc.Query5Kernel;
import bench.queries.impl.ldbc.Query6Kernel;

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

    public void run( String[] argv ) throws IOException
    {
        if ( argv.length == 0 )
        {
            System.out.println( "Expected input of combination of query2, query4, query5, query6" );
            System.exit( 1 );
        }

//        String dbName = Config.LDBC_SF001; // LDBC
        String dbName = Config.LAB_8.dbName; // LAB_8

        GraphDatabaseService graphDb = GraphDatabaseProvider.openDatabase( Config.GRAPH_DB_FOLDER, dbName );

        for ( String arg : argv )
        {
            switch ( arg )
            {
            case "query2":
                generateInputForQuery( graphDb, new Query2Kernel() );
                break;
            case "query4":
                generateInputForQuery( graphDb, new Query4Kernel() );
                break;
            case "query5":
                generateInputForQuery( graphDb, new Query5Kernel() );
                break;
            case "query6":
                generateInputForQuery( graphDb, new Query6Kernel() );
                break;
            case "labquery1":
                generateInputForQuery( graphDb, new LabQuery1Kernel( 1 ) );
                break;
            default:
                System.out.println( "Unexpected input. Only accept query2, query4, query5, query6" );
            }
        }
    }

    private void generateInputForQuery( GraphDatabaseService graphDb, Query query ) throws IOException
    {
        System.out.println( "Create input data for query with input header "
                            + Arrays.toString( query.inputDataHeader() ) );
        System.out.print( "Open file " + query.inputFile() + "... " );
        PrintWriter out = new PrintWriter( openOutputStream( query.inputFile() ) );
        System.out.print( "ok\n");
        System.out.print( "Generate random ids... " );
        List<Long> personIds = generateRandomIdsForLabel( graphDb, query.inputDataHeader()[0] );
        System.out.print( "ok\n" );

        char separator = InputDataLoader.csvSeparator;

        // HEADER
        String[] header = query.inputDataHeader();
        printHeader( out, header, separator );

        // ... and shuffle the order and write to file...
        // CONTENT
        Collections.shuffle( personIds );
        System.out.print( "Writing to file..." );
        personIds.forEach( out::println );
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

    public static void main( String[] argv ) throws IOException
    {
        new InputDataGenerator().run( argv );
    }
}
