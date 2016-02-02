package nl.trifork.blog.timelinedata.mapper;

import com.google.common.collect.Iterators;
import nl.trifork.blog.timelinedata.DataPoint;

import java.util.Iterator;
import java.util.List;

public class MongodbTimelineMapper implements TimelineMapper {

    public void storeDataPoints(Iterator<DataPoint> dataPoints) {
        Iterators.partition(dataPoints, 1000)
                .forEachRemaining(dataPointBatch -> {});

    }

    public List<DataPoint> getDataPoints(String sensorId, long startTimestamp,
                                         long endTimestamp, int limit) {
        // TODO Auto-generated method stub
        return null;
    }

}
