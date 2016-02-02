package nl.trifork.blog.timelinedata.mapper;

import nl.trifork.blog.timelinedata.DataPoint;

import java.util.Iterator;
import java.util.List;

public interface TimelineMapper {

    public void storeDataPoints(Iterator<DataPoint> dataPoints);

    public List<DataPoint> getDataPoints(String sensorId, long startTimestamp, long endTimestamp, int limit);

}
