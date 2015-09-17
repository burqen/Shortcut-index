package index.logical.util;

public interface Function<T, R>
{
    /**
     * Apply a value to this function
     *
     * @param t the function argument
     * @return the function result
     */
    R apply( T t );
}
