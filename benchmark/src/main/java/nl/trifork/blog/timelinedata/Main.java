package nl.trifork.blog.timelinedata;

import nl.trifork.blog.timelinedata.mapper.CassandraTimelineMapper;
import nl.trifork.blog.timelinedata.mapper.TimelineMapper;

import java.util.Collections;
import java.util.List;
import java.util.UUID;



public class Main {

    public static void main(String[] args) {
        DataPointIterator dataPointIterator = new DataPointIterator(25000, 1024, 1000, 0);
        TimelineMapper mapper = new CassandraTimelineMapper();
        mapper.storeDataPoints(dataPointIterator);

        long start = System.currentTimeMillis();

        List<UUID> sensorIds = dataPointIterator.getSensorIds();
        Collections.shuffle(sensorIds);
        // Read back all the sensor data in random order
        for (UUID sensorId : sensorIds) {
            mapper.getDataPoints(sensorId.toString(), -1, -1, -1);
        }

        System.out.println(String.format("Reading back the data took %s ms", System.currentTimeMillis() - start));
    }


}
