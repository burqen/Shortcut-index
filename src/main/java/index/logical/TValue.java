package index.logical;

public class TValue
{
    long relId;
    long nodeId;

    public TValue( long relId, long nodeId )
    {
        this.relId = relId;
        this.nodeId = nodeId;
    }

    @Override
    public int hashCode() {
        return (int) (relId * 23 + nodeId);
    }

    @Override
    public boolean equals( Object obj ) {
        if ( !( obj instanceof TValue ) )
            return false;
        if ( obj == this )
            return true;

        TValue rhs = (TValue) obj;
        return relId == rhs.relId && nodeId == rhs.nodeId;
    }
}
