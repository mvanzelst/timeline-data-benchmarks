package nl.hva.iot.benchmark.storage;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraTimelineStore implements TimelineStore {

	private final Cluster cluster;

	public CassandraTimelineStore() {
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
	}
	
	public void initSchema() {
		dropTablesAndKeySpace();
		createTablesAndKeySpace();
	}

	public Object getConnection() {
		return cluster.connect("iot_benchmark");
	}
	
	private void createTablesAndKeySpace(){
		Session session = cluster.connect();

		session.execute(
			"CREATE KEYSPACE IF NOT EXISTS iot_benchmark WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };"
		);

		session.close();
		session = cluster.connect("iot_benchmark");

		// Create event_counters table
		session.execute(
			"CREATE TABLE IF NOT EXISTS sensor_data ( " + 
				"sensor_id text, " +
				"data blob, " +
				"timestamp bigint, " +
				"PRIMARY KEY (sensor_id, timestamp) " +
			")"
		);

		session.close();
	}

	private void dropTablesAndKeySpace(){
		Session session = cluster.connect();
		session.execute("DROP KEYSPACE IF EXISTS iot_benchmark");
		session.close();
	}

}
