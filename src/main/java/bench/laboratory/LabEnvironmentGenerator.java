package bench.laboratory;

import bench.util.Config;
import bench.util.GraphDatabaseProvider;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class LabEnvironmentGenerator
{
    public static final String creationDate = "date";

    enum Nodes implements Label
    {
        Person,
        Comment
    }

    enum Rels implements RelationshipType
    {
        CREATED
    }

    /**
     * Generate a lab environment following the lab scheme.
     * Will use a uniform distribution for creationDate on Comments. From (including) 2010 to (excluding) 2013
     * @param path  {@link String} directory where generated database should be stored
     * @paran lab   {@link Lab} deciding parameters for dataset
     * @param out   {@link PrintStream} to print report to
     */
    public static void generate( String path, Lab lab, PrintStream out )
    {
        Calendar cal = new GregorianCalendar();
        cal.set( 2010, Calendar.JANUARY, 1 );
        long lowerBoundary = cal.getTimeInMillis();
        cal.set( 2013, Calendar.JANUARY, 1 );
        long upperBoundary = cal.getTimeInMillis();

        Random rnd = new Random();

        String datasetName = lab.dbName;
        GraphDatabaseService graphDb = GraphDatabaseProvider.openDatabase( path, datasetName );

        List<Node> persons = new ArrayList<>();

        int transactionSize = 1000;

        // GENERATE PERSONS
        int createdPersons = 0;
        while ( createdPersons < lab.nbrOfPersons )
        {
            try ( Transaction tx = graphDb.beginTx() )
            {
                int inThisTransaction = 0;
                while ( createdPersons + inThisTransaction < lab.nbrOfPersons && inThisTransaction < transactionSize )
                {
                    persons.add( graphDb.createNode( Nodes.Person ) );
                    inThisTransaction++;
                }
                createdPersons += inThisTransaction;
                tx.success();
            }
        }

        // GENERATE COMMENTS PER PERSON
        Iterator<Node> personsIterator = persons.iterator();
        int createdComments = 0;
        while ( personsIterator.hasNext() )
        {
            Node person = personsIterator.next();
            int createdCommentsForThisPerson = 0;
            while ( createdCommentsForThisPerson < lab.fanOut )
            {
                try ( Transaction tx = graphDb.beginTx() )
                {
                    int inThisTransaction = 0;
                    while ( inThisTransaction < transactionSize &&
                            createdCommentsForThisPerson + inThisTransaction < lab.fanOut )
                    {
                        Node comment = graphDb.createNode( Nodes.Comment );
                        long date = lowerBoundary + (long) (rnd.nextDouble() * ( upperBoundary - lowerBoundary ));
                        comment.setProperty( creationDate, date );
                        person.createRelationshipTo( comment, Rels.CREATED );
                        inThisTransaction++;
                    }
                    createdCommentsForThisPerson += inThisTransaction;
                    tx.success();
                }
            }
            createdComments += createdCommentsForThisPerson;
        }

        out.print(
                String.format( "Generated dataset\nPath: %s\nPersons: %d\nComments: %d\nFanOut: %d\n",
                        path + datasetName, createdPersons, createdComments, lab.fanOut )
        );
    }

    public static void main( String[] args )
    {
        String path = Config.GRAPH_DB_FOLDER;
        Lab lab = Config.LAB_10;
        LabEnvironmentGenerator.generate( path, lab, System.out );
    }
}
