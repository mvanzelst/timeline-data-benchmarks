package nl.hva.iot.benchmark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import nl.hva.iot.benchmark.service.DataPointCollection;
import nl.hva.iot.benchmark.storage.mapper.CassandraTimelineMapper;
import nl.hva.iot.benchmark.storage.mapper.TimelineMapper;
import nl.hva.iot.benchmark.model.DataPoint;;


public class Main {
	
	private final int batchSize = 1000;

	public static void main(String[] args) {
		int gb = 25;
		DataPointCollection dataPointCollection = new DataPointCollection(50, 1024, 1000 * gb, 0);
		long size = dataPointCollection.size();
		
		TimelineMapper mapper = new CassandraTimelineMapper();
		
		long counter = 0;
		List<DataPoint> batch = new ArrayList<DataPoint>();
		for (DataPoint dataPoint : dataPointCollection) {
			batch.add(dataPoint);
			counter++;
			
			if(batch.size() >= 1000){
				mapper.storeDataPoints(batch);
				batch = new ArrayList<DataPoint>();
				System.out.println(String.format("%s/%s=%s%%", counter, size, ((double) counter / size) * 100));
			}
		}
		mapper.storeDataPoints(batch);
		
		long start = System.currentTimeMillis();
		
		// Read back all the sensor data in random order
		// TODO randomize
		for (UUID sensorId : dataPointCollection.getSensorIds()) {
			mapper.getDataPoints(sensorId.toString(), -1, -1, -1);
		}
		
		System.out.println(String.format("Reading back the data took %s ms", System.currentTimeMillis() - start));
		System.exit(0);
	}
	
	

}
