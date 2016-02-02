package nl.trifork.blog.timelinedata.service;

import nl.trifork.blog.timelinedata.model.DataPoint;

import java.util.*;
import java.util.function.Consumer;

public class DataPointCollection implements Iterable<DataPoint> {

    private final long dataSizePerDataPoint;
    private final long numberOfDataPointsPerSensor;
    private final long startTime;

    private final List<UUID> sensorIds;
    private final long totalDatapoints;

    public DataPointCollection(long numberOfSensors, long dataSizePerDataPoint, long numberOfDataPointsPerSensor, long startTime) {
        this.dataSizePerDataPoint = dataSizePerDataPoint;
        this.startTime = startTime;
        this.numberOfDataPointsPerSensor = numberOfDataPointsPerSensor;

        sensorIds = new ArrayList<UUID>();
        for (int i = 0; i < numberOfSensors; i++) {
            sensorIds.add(UUID.randomUUID());
        }

        totalDatapoints = numberOfSensors * numberOfDataPointsPerSensor;
    }

    public long size() {
        return totalDatapoints;
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
        byte[] b = new byte[(int) dataSizePerDataPoint];
        new Random().nextBytes(b);
        return b;
    }

    public Iterator<DataPoint> iterator() {
        return new Iterator<DataPoint>() {
            private long counter = 0;

            public boolean hasNext() {
                return counter < totalDatapoints;
            }

            public DataPoint next() {
                return generateDataPoint(counter++);
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }

            public void forEachRemaining(Consumer<? super DataPoint> action) {
                throw new UnsupportedOperationException();
            }
        };
    }

    public void forEach(Consumer<? super DataPoint> action) {
        throw new UnsupportedOperationException();
    }

    public Spliterator<DataPoint> spliterator() {
        throw new UnsupportedOperationException();
    }

}
