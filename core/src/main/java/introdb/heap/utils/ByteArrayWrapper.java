package introdb.heap.utils;

import java.util.Arrays;

public class ByteArrayWrapper {

    private final byte[] data;

    private ByteArrayWrapper(byte[] data) {
        if (data == null) {
            throw new NullPointerException();
        }
        this.data = data;
    }

    public static ByteArrayWrapper of(byte[] data) {
        return new ByteArrayWrapper(data);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ByteArrayWrapper that = (ByteArrayWrapper) o;
        return Arrays.equals(data, that.data);
    }

    // Arrays.hashCode have massive number of collisions
    // https://bugs.openjdk.java.net/browse/JDK-8134141
    @Override
    public int hashCode() {
        if (data == null)
            return 0;

        int result = 1;
        for (byte element : data)
            result = 257 * result + element;

        return result;
    }

    public byte[] getData() {
        return data;
    }
}
