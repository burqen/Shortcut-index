package bench.laboratory;

public class Lab
{
    public final String dbName;
    public final int fanOut;
    public final int nbrOfPersons;

    public Lab( String dbName, int fanOut, int nbrOfPersons )
    {
        this.dbName = dbName;
        this.fanOut = fanOut;
        this.nbrOfPersons = nbrOfPersons;
    }
}
