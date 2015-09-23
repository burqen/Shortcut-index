package bench.util;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class GraphDatabaseProvider
{

    public static GraphDatabaseService openDatabase( String path, String dbName )
    {
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( path + dbName )
                .setConfig( "allow_store_upgrade", "true" )
                .newGraphDatabase();
        registerShutdownHook( graphDb );

        return graphDb;
    }


    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
}
