package nl.hva.iot.benchmark.storage.mapper;

import java.util.List;

import nl.hva.iot.benchmark.model.DataPoint;

public interface TimelineMapper {

	public void storeDataPoints(List<DataPoint> datapoint);
	
	public List<DataPoint> getDataPoints(String sensorId, long startTimestamp, long endTimestamp, int limit);
	
}
