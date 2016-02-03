package nl.trifork.blog.timelinedata.mapper;

import nl.trifork.blog.timelinedata.DataPoint;
import nl.trifork.blog.timelinedata.DataPointIterator;

import java.util.List;

public interface TimelineMapper {

    public void storeDataPoints(DataPointIterator dataPointIterator);

    public List<DataPoint> getDataPoints(int sensorId, long startTimestamp, long endTimestamp, int limit);

}
