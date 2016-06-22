package nl.trifork.blog.timelinedata;

import java.util.Iterator;
import java.util.Random;

/**
 * Iterates DataPoints chronologically
 */
public class DataPointIterator implements Iterator<DataPoint> {

    private final int dataSizePerDataPoint;
    private final long startTime;
    private final int totalDatapoints;
    private final int numberOfSensors;
    private long counter = 0;

    public DataPointIterator(int numberOfSensors, int dataSizePerDataPointInBytes, int numberOfDataPointsPerSensor, long startTime) {
        this.dataSizePerDataPoint = dataSizePerDataPointInBytes;
        this.startTime = startTime;
        this.totalDatapoints = numberOfSensors * numberOfDataPointsPerSensor;
        this.numberOfSensors = numberOfSensors;
    }

    private DataPoint generateDataPoint(long counter) {
        int sensorId = (int) (counter % numberOfSensors);
        long timestamp = counter / numberOfSensors;
        return new DataPoint(sensorId, startTime + timestamp, generateData());
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

    public long size(){
        return totalDatapoints;
    }
}
