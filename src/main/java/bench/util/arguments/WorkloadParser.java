package bench.util.arguments;

import bench.Workload;
import bench.queries.Query;
import bench.queries.impl.lab.LabQuery1Kernel;
import bench.queries.impl.lab.LabQuery1Shortcut;
import bench.queries.impl.ldbc.Query1Kernel;
import bench.queries.impl.ldbc.Query1Shortcut;
import bench.queries.impl.ldbc.Query2Kernel;
import bench.queries.impl.ldbc.Query2Shortcut;
import bench.queries.impl.ldbc.Query3Kernel;
import bench.queries.impl.ldbc.Query3Shortcut;
import bench.queries.impl.ldbc.Query4Kernel;
import bench.queries.impl.ldbc.Query4Shortcut;
import bench.queries.impl.ldbc.Query5Kernel;
import bench.queries.impl.ldbc.Query5Shortcut;
import bench.queries.impl.ldbc.Query6Kernel;
import bench.queries.impl.ldbc.Query6Shortcut;
import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.StringParser;

import static bench.Environment.LAB;
import static bench.Environment.LDBC;

public class WorkloadParser extends StringParser
{
    @Override
    public Object parse( String s ) throws ParseException
    {
        Query[] shortcutLDBCQueries = new Query[]
                {
                        new Query1Shortcut(),
                        new Query2Shortcut(),
                        new Query3Shortcut(),
                        new Query4Shortcut(),
                        new Query5Shortcut(),
                        new Query6Shortcut(),
                };

        Query[] kernelLDBCQueries = new Query[]
                {
                        new Query1Kernel(),
                        new Query2Kernel(),
                        new Query3Kernel(),
                        new Query4Kernel(),
                        new Query5Kernel(),
                        new Query6Kernel()
                };
        Query[] shortcutLABQueries = new Query[]
                {
                    new LabQuery1Shortcut( 1 )
                };

        Query[] kernelLABQueries = new Query[]
                {
                        new LabQuery1Kernel( 1 )
                };

        Workload workload;
        switch ( s )
        {
        case "ldbcall":
            workload = new Workload( LDBC );
            workload.addQueries( shortcutLDBCQueries );
            workload.addQueries( kernelLDBCQueries );
            break;
        case "ldbc1":
            workload = new Workload( LDBC );
            workload.addQuery( shortcutLDBCQueries[0] );
            workload.addQuery( kernelLDBCQueries[0] );
            break;
        case "ldbc2":
            workload = new Workload( LDBC );
            workload.addQuery( shortcutLDBCQueries[1] );
            workload.addQuery( kernelLDBCQueries[1] );
            break;
        case "ldbc3":
            workload = new Workload( LDBC );
            workload.addQuery( shortcutLDBCQueries[2] );
            workload.addQuery( kernelLDBCQueries[2] );
            break;
        case "ldbc4":
            workload = new Workload( LDBC );
            workload.addQuery( shortcutLDBCQueries[3] );
            workload.addQuery( kernelLDBCQueries[3] );
            break;
        case "ldbc5":
            workload = new Workload( LDBC );
            workload.addQuery( shortcutLDBCQueries[4] );
            workload.addQuery( kernelLDBCQueries[4] );
            break;
        case "ldbc6":
            workload = new Workload( LDBC );
            workload.addQuery( shortcutLDBCQueries[5] );
            workload.addQuery( kernelLDBCQueries[5] );
            break;
        case "laball":
            workload = new Workload( LAB );
            workload.addQueries( shortcutLABQueries );
            workload.addQueries( kernelLABQueries );
            break;
        case "lab1":
            workload = new Workload( LAB );
            workload.addQuery( shortcutLABQueries[0] );
            workload.addQuery( kernelLABQueries[0] );
            break;
        default:
            throw new IllegalArgumentException( "Can not create workload from argument: " + s );

//            "lab100 | lab75 | lab50 | lab 25 | lab1>
        }
        workload.buildIndexDescriptions();
        return workload;
    }

    public static final WorkloadParser INSTANCE = new WorkloadParser();
}
