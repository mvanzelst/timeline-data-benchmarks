package nl.trifork.blog.timelinedata.storage.mapper;

import nl.trifork.blog.timelinedata.model.DataPoint;

import java.util.List;

public interface TimelineMapper {

    public void storeDataPoints(List<DataPoint> datapoint);

    public List<DataPoint> getDataPoints(String sensorId, long startTimestamp, long endTimestamp, int limit);

}
