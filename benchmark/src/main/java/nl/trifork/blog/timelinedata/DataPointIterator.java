package nl.trifork.blog.timelinedata;

import java.util.*;

public class DataPointIterator implements Iterator<DataPoint> {

    private final int dataSizePerDataPoint;
    private final long startTime;
    private final long totalDatapoints;
    private long counter = 0;

    private final List<UUID> sensorIds;

    public DataPointIterator(long numberOfSensors, int dataSizePerDataPointInBytes, long numberOfDataPointsPerSensor, long startTime) {
        this.dataSizePerDataPoint = dataSizePerDataPointInBytes;
        this.startTime = startTime;

        sensorIds = new ArrayList<>();
        for (int i = 0; i < numberOfSensors; i++) {
            sensorIds.add(UUID.randomUUID());
        }

        totalDatapoints = numberOfSensors * numberOfDataPointsPerSensor;
    }

    public List<UUID> getSensorIds() {
        return sensorIds;
    }

    private DataPoint generateDataPoint(long counter) {
        long sensorId = counter % sensorIds.size();
        long timestamp = counter / sensorIds.size();
        return new DataPoint(sensorIds.get((int) sensorId).toString(), startTime + timestamp, generateData());
    }

    private byte[] generateData() {
        byte[] b = new byte[dataSizePerDataPoint];
        new Random().nextBytes(b);
        return b;
    }

    @Override
    public boolean hasNext() {
        return counter < totalDatapoints;
    }

    @Override
    public DataPoint next() {
        return generateDataPoint(counter++);
    }
}
