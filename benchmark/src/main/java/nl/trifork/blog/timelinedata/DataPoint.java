package nl.trifork.blog.timelinedata;

import org.apache.commons.codec.digest.DigestUtils;

public class DataPoint {

    private final int sensorId;
    private final long timestamp;
    private final byte[] data;

    public DataPoint(int sensorId, long timestamp, byte[] data) {
        this.sensorId = sensorId;
        this.timestamp = timestamp;
        this.data = data;
    }

    @Override
    public String toString() {
        return String.format(
                "sensorId: %s, timestamp: %s, dataChecksum: %s",
                getSensorId(),
                getTimestamp(),
                DigestUtils.md5Hex(getData())
        );
    }

    public int getSensorId() {
        return sensorId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] getData() {
        return data;
    }
}
