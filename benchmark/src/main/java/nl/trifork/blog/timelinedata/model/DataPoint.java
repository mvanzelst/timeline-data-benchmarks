package nl.trifork.blog.timelinedata.model;

import org.apache.commons.codec.digest.DigestUtils;

public class DataPoint {

    public final String sensorId;
    public final long timestamp;
    public final byte[] data;

    public DataPoint(String sensorId, long timestamp, byte[] data) {
        this.sensorId = sensorId;
        this.timestamp = timestamp;
        this.data = data;
    }

    @Override
    public String toString() {
        return String.format(
                "sensorId: %s, timestamp: %s, dataChecksum: %s",
                sensorId,
                timestamp,
                DigestUtils.md5Hex(data)
        );
    }

}
