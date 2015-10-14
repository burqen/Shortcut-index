package bench.util;

import au.com.bytecode.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static bench.util.Config.NO_DATA;
import static bench.util.Config.NO_HEADER;
import static bench.util.Config.NO_INPUT;

public class InputDataLoader
{
    public static char csvSeparator = '|';

    public List<long[]> load( String dataFileName, String[] expectedHeader, int inputSize ) throws FileNotFoundException
    {
        List<long[]> inputData = new ArrayList<>();
        if ( !dataFileName.equals( NO_INPUT ) )
        {
            File file = new File( dataFileName );
            BufferedReader reader = new BufferedReader( new FileReader( file ) );
            String[] line;

            try ( CSVReader csvReader = new CSVReader( reader, csvSeparator ) )
            {
                String[] actualHeader = csvReader.readNext();
                if ( !Arrays.equals( expectedHeader, actualHeader ) )
                {
                    return null;
                }

                // Ready to read input
                int count = 0;
                while ( (line = csvReader.readNext()) != null && count++ < inputSize )
                {
                    long[] input = new long[line.length];
                    for ( int i = 0; i < line.length; i++ )
                    {
                        input[i] = Long.parseLong( line[i] );
                    }
                    inputData.add( input );
                }
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                System.exit( 1 );
                return null;
            }
        }
        else
        {
            if ( Arrays.equals( expectedHeader, NO_HEADER ) )
            {
                inputData.add( NO_DATA );
            }
            else
            {
                return null;
            }
        }
        return inputData;
    }
}