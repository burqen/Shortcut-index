package bench;

import bench.queries.Query1Kernel;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

public class BenchmarkMain
{

    public static void main( String[] argv )
    {
        new BenchmarkMain().run( argv );
    }

    private void run( String[] argv )
    {
        String resourcePath = "src/main/resources/";
        String dbPath = "ldbc_sf001_p006_Neo4jDb";

        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( resourcePath + dbPath ).newGraphDatabase();

//        try ( Transaction tx = graphDb.beginTx() )
//        {
//            ReadOperations readOperations = ((GraphDatabaseAPI) graphDb).getDependencyResolver()
//                    .resolveDependency( ThreadToStatementContextBridge.class )
//                    .get().readOperations();
//            Iterator<RelationshipType> allTypes =
//                    GlobalGraphOperations.at( graphDb ).getAllRelationshipTypes().iterator();
//
//            while ( allTypes.hasNext() )
//            {
//                RelationshipType next = allTypes.next();
//                System.out.println( next.name() );
//            }
//
//            PrimitiveLongIterator persons =
//                    readOperations.nodesGetForLabel( readOperations.labelGetForName( "Person" ) );
//
//            while ( persons.hasNext() )
//            {
//                long node = persons.next();
//                printNode( graphDb.getNodeById( node ) );
//            }
//            tx.success();
//        }
        Query1Kernel kernelQuery = new Query1Kernel();
        BenchLogger logger = new BenchLogger( System.out );
        kernelQuery.runQuery( graphDb, logger );
        logger.report();
    }

    private void createIndex( GraphDatabaseService graphDb, String label, String prop )
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
        }
    }

    private void printNode( Node node )
    {
        Iterator<Label> labels = node.getLabels().iterator();
        while ( labels.hasNext() )
        {
            Label label = labels.next();
            System.out.print( label.name() + " " );
        }
        System.out.println();

        Iterator<String> propKeys = node.getPropertyKeys().iterator();
        while ( propKeys.hasNext() )
        {
            String propKey = propKeys.next();
            System.out.print( "    " + propKey + ": " );
            System.out.println( node.getProperty( propKey ) );
        }
        System.out.println();
    }
}
