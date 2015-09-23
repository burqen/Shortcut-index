package bench.util;

import bench.queries.Query2;
import bench.queries.framework.BaseQuery;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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
import org.neo4j.tooling.GlobalGraphOperations;

public class InputDataGenerator
{

    public void run( String[] argv ) throws IOException
    {
        String dbName = Config.LDBC_SF001;

        GraphDatabaseService graphDb = GraphDatabaseProvider.openDatabase( Config.GRAPH_DB_FOLDER, dbName );

        // Query 2
        // Person
        BaseQuery query = new Query2();

        PrintWriter out = new PrintWriter( openOutputStream( Config.QUERY2_PARAMETERS ) );

        int numberOfPersons = 0;
        try ( Transaction tx = graphDb.beginTx() )
        {
            ReadOperations operations = ((GraphDatabaseAPI) graphDb).getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class )
                    .get().readOperations();

            int personLabelId = operations.labelGetForName( "Person" );
            PrimitiveLongIterator nodesWithLabelPerson = operations.nodesGetForLabel( personLabelId );

            while ( nodesWithLabelPerson.hasNext() )
            {
                nodesWithLabelPerson.next();
                numberOfPersons++;
            }

            tx.success();
        }

        int desired = Config.MAX_NUMBER_OF_QUERY_REPETITIONS;
        Random rng = new Random();
        Set<Integer> personsToAdd = new TreeSet<>();

        while ( personsToAdd.size() < desired )
        {
            personsToAdd.add( rng.nextInt( numberOfPersons ) );
        }

        List<Long> personIds = new ArrayList<>();
        try ( Transaction tx = graphDb.beginTx() )
        {
            ReadOperations operations = ((GraphDatabaseAPI) graphDb).getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class )
                    .get().readOperations();

            int personLabelId = operations.labelGetForName( "Person" );
            PrimitiveLongIterator nodesWithLabelPerson = operations.nodesGetForLabel( personLabelId );

            int count = 0;
            Iterator<Integer> personsToAddIt = personsToAdd.iterator();
            int nextToAdd = personsToAddIt.next();
            while ( nodesWithLabelPerson.hasNext() )
            {
                long nextId = nodesWithLabelPerson.next();
                if ( count == nextToAdd )
                {
                    personIds.add( nextId );
                    if ( !personsToAddIt.hasNext() )
                    {
                        break;
                    }
                    nextToAdd = personsToAddIt.next();
                }
                count++;
            }

            tx.success();
        }



        char separator = InputDataLoader.csvSeparator;

        // HEADER
        String[] header = query.inputDataHeader();
        printHeader( out, header, separator );

        // CONTENT
        Collections.shuffle( personIds );
        personIds.forEach( out::println );

        out.close();
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
        FileOutputStream out = null;

        file = new File( filePath );

        // if file doesnt exists, then create it
        if ( !file.exists() )
        {
            file.createNewFile();
        }

        out = new FileOutputStream( file );

        return out;
    }

    public static void main( String[] argv ) throws IOException
    {
        new InputDataGenerator().run( argv );
    }
}
