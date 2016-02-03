package nl.trifork.blog.timelinedata;

import nl.trifork.blog.timelinedata.mapper.MongodbTimelineMapper;
import nl.trifork.blog.timelinedata.mapper.TimelineMapper;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class Main {

    public static void main(String[] args) {
        int numSensors = 200;
        int numDataPointsPerSensor = 1000;
        DataPointIterator dataPointIterator = new DataPointIterator(numSensors, 1024, numDataPointsPerSensor, 0);
//        TimelineMapper mapper = new CassandraTimelineMapper();
        TimelineMapper mapper = new MongodbTimelineMapper();
        mapper.storeDataPoints(dataPointIterator);

        List<Integer> sensorIds = IntStream.range(0, numSensors).boxed().collect(Collectors.toList());
        Collections.shuffle(sensorIds);
        long start = System.currentTimeMillis();
        // Read back all the sensor data in random order
        for (Integer sensorId : sensorIds) {
            List<DataPoint> dataPoints = mapper.getDataPoints(sensorId);
            if(dataPoints.size() != numDataPointsPerSensor){
                throw new RuntimeException("Failed to read all data points");
            }
        }
        System.out.println(String.format("Reading back the data took %s ms", System.currentTimeMillis() - start));
    }


}
