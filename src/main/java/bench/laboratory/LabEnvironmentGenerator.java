package bench.laboratory;

import bench.util.Config;
import bench.util.GraphDatabaseProvider;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class LabEnvironmentGenerator
{
    public static final int RANGE_MAX = 8000;
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
     * @param path  {@link String} directory where generated database should be stored
     * @paran lab   {@link Lab} deciding parameters for dataset
     * @param out   {@link PrintStream} to print report to
     */
    public static void generate( String path, Lab lab, PrintStream out )
    {
        int delta = RANGE_MAX / lab.fanOut;

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
                            createdCommentsForThisPerson < lab.fanOut )
                    {
                        Node comment = graphDb.createNode( Nodes.Comment );
                        int date = createdCommentsForThisPerson * delta;
                        comment.setProperty( creationDate, date );
                        person.createRelationshipTo( comment, Rels.CREATED );
                        inThisTransaction++;
                        createdCommentsForThisPerson++;
                        createdComments++;
                    }
                    tx.success();
                }
            }
        }

        out.print(
                String.format( "Generated dataset\nPath: %s\nPersons: %d\nComments: %d\nFanOut: %d\n",
                        path + datasetName, createdPersons, createdComments, lab.fanOut )
        );
    }

    public static void main( String[] args )
    {
        String path = Config.GRAPH_DB_FOLDER;
        LabEnvironmentGenerator.generate( path, Config.LAB_8, System.out );
        LabEnvironmentGenerator.generate( path, Config.LAB_40, System.out );
        LabEnvironmentGenerator.generate( path, Config.LAB_200, System.out );
        LabEnvironmentGenerator.generate( path, Config.LAB_400, System.out );
        LabEnvironmentGenerator.generate( path, Config.LAB_800, System.out );
    }
}
