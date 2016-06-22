package nl.trifork.blog.timelinedata;

import nl.trifork.blog.timelinedata.mapper.CassandraTimelineMapper;
import nl.trifork.blog.timelinedata.mapper.TimelineMapper;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class Main {

    public static void main(String[] args) throws InterruptedException {

        int numSensors = 25000;
        int numDataPointsPerSensor = 10000;

        // Create an iterator that iterates the given amount of sensors and amount of datapoints per sensor chronologically
        DataPointIterator dataPointIterator = new DataPointIterator(numSensors, 100, numDataPointsPerSensor, 0);


        TimelineMapper mapper = new CassandraTimelineMapper(true);
//        TimelineMapper mapper = new MongodbTimelineMapper(true);

        // Store the data
        mapper.storeDataPoints(dataPointIterator);

        List<Integer> sensorIds = IntStream.range(0, numSensors).boxed().collect(Collectors.toList());
        Collections.shuffle(sensorIds);

        long startTotal = System.currentTimeMillis();

        ExecutorService executorService = Executors.newFixedThreadPool(25);


        // Read back all the sensor data in random order
        for (Integer sensorId : sensorIds) {
            executorService.execute(() -> {
                long start = System.currentTimeMillis();
                List<DataPoint> dataPoints = mapper.getDataPoints(sensorId);
                if(dataPoints.size() != numDataPointsPerSensor){
                    throw new RuntimeException("Failed to read all data points");
                }
                System.out.println(String.format("Reading back data took %s ms for sensor %s",
                        System.currentTimeMillis() - start, sensorId));
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.DAYS);
        System.out.println(String.format("Reading back all data took %s ms",
                System.currentTimeMillis() - startTotal));
        System.exit(0);
    }


}
