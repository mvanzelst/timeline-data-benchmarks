package nl.trifork.blog.timelinedata.store;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraTimelineStore implements TimelineStore {

    private final Cluster cluster;

    public CassandraTimelineStore() {
        cluster = Cluster.builder().addContactPoint("172.17.0.2").build();
    }

    public void initSchema() {
        dropTablesAndKeySpace();
        createTablesAndKeySpace();
    }

    public Object getConnection() {
        return cluster.connect("time_series_benchmark");
    }

    private void createTablesAndKeySpace() {
        Session session = cluster.connect();

        session.execute(
                "CREATE KEYSPACE IF NOT EXISTS time_series_benchmark WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };"
        );

        session.close();
        session = cluster.connect("time_series_benchmark");

        // Create event_counters table
        session.execute(
                "CREATE TABLE IF NOT EXISTS sensor_data ( " +
                        "sensor_id int, " +
                        "data blob, " +
                        "timestamp bigint, " +
                        "PRIMARY KEY (sensor_id, timestamp) " +
                        ")"
        );

        session.close();
    }

    private void dropTablesAndKeySpace() {
        Session session = cluster.connect();
        session.execute("DROP KEYSPACE IF EXISTS time_series_benchmark");
        session.close();
    }

}
