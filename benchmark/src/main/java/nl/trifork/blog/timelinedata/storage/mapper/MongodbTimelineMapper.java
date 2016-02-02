package nl.trifork.blog.timelinedata.storage.mapper;

import nl.trifork.blog.timelinedata.model.DataPoint;

import java.util.List;

public class MongodbTimelineMapper implements TimelineMapper {

    public void storeDataPoints(List<DataPoint> datapoint) {
        // TODO Auto-generated method stub

    }

    public List<DataPoint> getDataPoints(String sensorId, long startTimestamp,
                                         long endTimestamp, int limit) {
        // TODO Auto-generated method stub
        return null;
    }

}
