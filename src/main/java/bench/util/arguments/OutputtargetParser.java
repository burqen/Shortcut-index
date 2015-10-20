package bench.util.arguments;

import bench.util.Config;
import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.StringParser;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class OutputtargetParser extends StringParser
{

    @Override
    public Object parse( String s ) throws ParseException
    {
        switch ( s )
        {
        case "system":
            return System.out;
        case "":
            throw new IllegalArgumentException( "Can not output to " + s );
        default:
            try
            {
                return openOutputStream( s );
            }
            catch ( IOException e )
            {
                ParseException parseException = new ParseException( "Error when trying to open PrintStream to file. " +
                                                                    "See attached cause." );
                parseException.addSuppressed( e );
                throw parseException;
            }
        }
    }

    public PrintStream openOutputStream( String fileName )
            throws IOException
    {
        File file;
        FileOutputStream out;

        file = new File( Config.OUTPUT_PATH + fileName );

        // if file doesnt exists, then create it
        if ( !file.exists() )
        {
            file.createNewFile();
        }

        out = new FileOutputStream( file, true );

        return new PrintStream( out );
    }

    public static OutputtargetParser INSTANCE = new OutputtargetParser();
}
