package nl.hva.iot.benchmark.storage;

public interface TimelineStore {

	public void initSchema();
	
	public Object getConnection();
}
