package bench.util;

import bench.BenchConfig;
import bench.IndexBuildLogger;
import index.SCIndex;
import index.SCIndexDescription;
import index.SCIndexProvider;
import index.btree.Index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.tooling.GlobalGraphOperations;

public class IndexLoader
{
    private static final String outPrefix = "[IndexLoader] ";
    public static SCIndexProvider loadIndexes( GraphDatabaseService graphDb, PageCache pageCache, String indexPath,
            BenchConfig benchConfig, Workload workload )
            throws IOException
    {
        SCIndexProvider provider = new SCIndexProvider();

        // Load existing indexes
        loadExistingIndexes( pageCache, indexPath, provider );

        // Create and populate not existing indexes
        Iterator<SCIndexDescription> indexDescriptions = workload.indexDescriptions();
        while ( indexDescriptions.hasNext() )
        {
            SCIndexDescription description = indexDescriptions.next();
            if ( !provider.contains( description ) )
            {
                createNewIndexForQuery( description, graphDb, pageCache, indexPath, benchConfig.pageSize(), provider );
            }
        }

        return provider;
    }

    private static void loadExistingIndexes( PageCache pageCache, String indexPath, SCIndexProvider provider )
            throws IOException
    {
        File indexDir = new File( indexPath );
        File[] filesArray = indexDir.listFiles( ( dir, name ) -> name.startsWith( SCIndex.filePrefix ) );
        if ( filesArray == null )
        {
            return;
        }
        List<File> files = Arrays.asList( filesArray );

        Map<String,File[]> indexAndMetaFiles = new HashMap<>();

        for ( File file : files )
        {
            String fileName = file.getName();
            if ( fileName.contains( SCIndex.filePrefix ) )
            {
                int index = 0;
                String prefixRemoved = fileName.replace( SCIndex.filePrefix, "" );
                String indexName;
                if ( prefixRemoved.contains( SCIndex.metaFileSuffix ) )
                {
                    // Meta file
                    indexName = prefixRemoved.replace( SCIndex.metaFileSuffix, "" );
                    index = 1;
                }
                else
                {
                    // Index file
                    indexName = prefixRemoved.replace( SCIndex.indexFileSuffix, "" );
                }
                if ( !indexAndMetaFiles.containsKey( indexName ) )
                {
                    indexAndMetaFiles.put( indexName, new File[2] );
                }
                indexAndMetaFiles.get( indexName )[index] = file;
            }
        }

        for ( Map.Entry<String,File[]> entry : indexAndMetaFiles.entrySet() )
        {
            File indexFile = entry.getValue()[0];
            File metaFile = entry.getValue()[1];
            print( String.format( "Loading index from %s %s\n",
                    indexFile.getName(), metaFile.getName() ) );
            print(  "Loading... " );
            SCIndex index = new Index( pageCache, indexFile, metaFile );
            printMidLine( String.format( "OK, Pattern: %s\n", index.getDescription() ) );
            provider.put( index );
        }
    }

    private static void createNewIndexForQuery( SCIndexDescription description, GraphDatabaseService graphDb,
            PageCache pageCache, String indexPath, int pageSize, SCIndexProvider indexes )
            throws IOException
    {
        print( "Build new index\n" );
        String prefix = SCIndex.filePrefix;
        String suffix = SCIndex.indexFileSuffix;
        String metaSuffix = SCIndex.metaFileSuffix;

        File indexDir = new File( indexPath );
        File[] files = indexDir.listFiles( ( dir, name ) -> name.startsWith( SCIndex.filePrefix )
                                                            && name.endsWith( SCIndex.metaFileSuffix ) );
        String indexNumber = Integer.toString( files.length );

        File indexFile = new File( indexPath + prefix + indexNumber + suffix );
        if ( !indexFile.exists() && !indexFile.createNewFile() )
        {
            throw new IOException( "Could not create new index file: " + indexFile.getName() );
        }

        File metaFile = new File( indexPath + prefix + indexNumber + metaSuffix );
        if ( !metaFile.exists() && !metaFile.createNewFile() )
        {
            throw new IOException( "Could not create new meta file: " + metaFile.getName() );
        }

        SCIndex index = new Index( pageCache, indexFile, metaFile, description, pageSize );

        populateShortcutIndex( graphDb, index, description );
        indexes.put( index );
    }

    public static void populateShortcutIndex( GraphDatabaseService graphDb, SCIndex index,
            SCIndexDescription desc, IndexBuildLogger logger ) throws IOException
    {
        if ( desc.nodePropertyKey != null )
        {
            populateShortcutIndex( graphDb, index, desc.firstLabel, desc.relationshipType, desc.direction,
                    desc.secondLabel, desc.nodePropertyKey, false, logger );

        }
        else
        {
            populateShortcutIndex( graphDb, index, desc.firstLabel, desc.relationshipType, desc.direction,
                    desc.secondLabel, desc.relationshipPropertyKey, true, logger );
        }
    }

    // TODO: Fix this to populate all indexes at once
    public static void populateShortcutIndex( GraphDatabaseService graphDb, SCIndex index, String firstLabelName,
            String relTypeName, Direction dir, String secondLabelName, String propName, boolean propOnRel,
            IndexBuildLogger logger ) throws IOException
    {
        print( String.format( "INDEX PATTERN: %s\n", index.getDescription() ) );
        print( "Building... " );
        int numberOfInsert = 0;
        try ( Transaction tx = graphDb.beginTx() )
        {
            Label firstLabel = DynamicLabel.label( firstLabelName );
            Label secondLabel = DynamicLabel.label( secondLabelName );

            for ( Relationship rel : GlobalGraphOperations.at( graphDb ).getAllRelationships() )
            {
                if ( rel.getType().name().equals( relTypeName ) )
                {
                    Node first;
                    Node second;
                    if ( dir == Direction.OUTGOING )
                    {
                        first = rel.getStartNode();
                        second = rel.getEndNode();
                    }
                    else
                    {
                        first = rel.getEndNode();
                        second = rel.getStartNode();
                    }
                    if ( first.hasLabel( firstLabel ) && second.hasLabel( secondLabel ) )
                    {
                        numberOfInsert++;
                        long prop = propOnRel ? ((Number) rel.getProperty( propName )).longValue() :
                                    ((Number) second.getProperty( propName )).longValue();

                        logger.startInsert();
                        index.insert( new long[]{first.getId(), prop}, new long[]{rel.getId(), second.getId()} );
                        logger.finishInsert();
                    }
                }
            }
            tx.success();
        }
        printMidLine( String.format( "OK [index size %d]\n", numberOfInsert ) );
    }

    private static void populateShortcutIndex( GraphDatabaseService graphDb, SCIndex index,
            SCIndexDescription desc ) throws IOException
    {
        populateShortcutIndex( graphDb, index, desc, IndexBuildLogger.NULL );
    }

    private static void print( String s )
    {
        System.out.print( outPrefix + s );
    }

    private static void printMidLine( String s )
    {
        System.out.print( s );
    }
}
