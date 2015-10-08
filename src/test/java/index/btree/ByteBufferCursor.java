package index.btree;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.PageCursor;

public class ByteBufferCursor implements PageCursor
{
    private final ByteBuffer buffer;

    public ByteBufferCursor( ByteBuffer buffer )
    {
        this.buffer = buffer;
    }

    @Override
    public short getShort()
    {
        return buffer.getShort();
    }

    @Override
    public void putShort( short value )
    {
        buffer.putShort( value );
    }

    @Override
    public short getShort( int index )
    {
        return buffer.getShort( index );
    }

    @Override
    public void putShort( int index, short value )
    {
        buffer.putShort( index, value );
    }

    @Override
    public void setOffset( int i )
    {
        buffer.position( i );
    }

    @Override
    public int getOffset()
    {
        return buffer.position();
    }

    @Override
    public long getCurrentPageId()
    {
        throw new NotImplementedException();
    }

    @Override
    public int getCurrentPageSize()
    {
        return buffer.capacity();
    }

    @Override
    public File getCurrentFile()
    {
        throw new NotImplementedException();
    }

    @Override
    public void rewind() throws IOException
    {
        buffer.position( 0 );
    }

    @Override
    public boolean next() throws IOException
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean next( long l ) throws IOException
    {
        throw new NotImplementedException();
    }

    @Override
    public void close()
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean shouldRetry() throws IOException
    {
        throw new NotImplementedException();
    }

    @Override
    public int getInt()
    {
        return buffer.getInt();
    }

    @Override
    public void putInt( int value )
    {
        buffer.putInt( value );
    }

    @Override
    public int getInt( int index )
    {
        return buffer.getInt( index );
    }

    @Override
    public void putInt( int index, int value )
    {
        buffer.putInt( index, value );
    }

    @Override
    public long getUnsignedInt()
    {
        return getInt() & 0xFFFFFFFFL;
    }

    @Override
    public long getUnsignedInt( int i )
    {
        return getInt( i ) & 0xFFFFFFFFL;
    }

    @Override
    public void getBytes( byte[] bytes )
    {
        buffer.get( bytes );
    }

    @Override
    public void getBytes( byte[] bytes, int offset, int length )
    {
        buffer.get( bytes, offset, length);
    }

    @Override
    public void putBytes( byte[] bytes )
    {
        buffer.put( bytes );
    }

    @Override
    public void putBytes( byte[] bytes, int offset, int length )
    {
        buffer.put( bytes, offset, length );
    }

    @Override
    public void putLong( long value )
    {
        buffer.putLong( value );
    }

    @Override
    public long getLong()
    {
        return buffer.getLong();
    }

    @Override
    public void putLong( int index, long value )
    {
        buffer.putLong( index, value );
    }

    @Override
    public long getLong( int index )
    {
        return buffer.getLong( index );
    }

    @Override
    public byte getByte()
    {
        return buffer.get();
    }

    @Override
    public void putByte( byte b )
    {
        buffer.put( b );
    }

    @Override
    public byte getByte( int index )
    {
        return buffer.get( index );
    }

    @Override
    public void putByte( int index, byte b )
    {
        buffer.put( index, b );
    }
}
