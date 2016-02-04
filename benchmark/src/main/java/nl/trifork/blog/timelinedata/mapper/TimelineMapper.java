package nl.trifork.blog.timelinedata.mapper;

import nl.trifork.blog.timelinedata.DataPoint;
import nl.trifork.blog.timelinedata.DataPointIterator;

import java.util.List;

public interface TimelineMapper {

    void storeDataPoints(DataPointIterator dataPointIterator);

    List<DataPoint> getDataPoints(int sensorId);

}
