package nl.trifork.blog.timelinedata.mapper;

import com.datastax.driver.core.*;
import com.google.common.collect.Iterators;
import nl.trifork.blog.timelinedata.DataPoint;
import nl.trifork.blog.timelinedata.DataPointIterator;
import nl.trifork.blog.timelinedata.store.CassandraTimelineStore;
import nl.trifork.blog.timelinedata.store.TimelineStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

public class CassandraTimelineMapper implements TimelineMapper {

    private static final Logger logger = LoggerFactory.getLogger(TimelineStore.class);

    private TimelineStore timelineStore;

    private final int batchSize = 100;

    public CassandraTimelineMapper() {
        timelineStore = new CassandraTimelineStore();
        timelineStore.initSchema();
    }

    public void storeDataPoints(DataPointIterator dataPointIterator) {
        Session session = (Session) timelineStore.getConnection();
        PreparedStatement ps = session.prepare(
                "INSERT INTO sensor_data (sensor_id, timestamp, data) VALUES (?, ?, ?)");


        AtomicInteger atomicInteger = new AtomicInteger();
        Iterators.partition(dataPointIterator, batchSize).forEachRemaining(dataPointBatch -> {
                BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                dataPointBatch.forEach(dataPoint ->
                        batchStatement.add(ps.bind(dataPoint.getSensorId(), dataPoint.getTimestamp(), ByteBuffer.wrap(dataPoint.getData()))));

                session.execute(batchStatement);
                logger.info("Inserted {} records out of {} - {}%",
                        atomicInteger.addAndGet(dataPointBatch.size()), dataPointIterator.size(),
                        ((double) atomicInteger.get() / dataPointIterator.size()) * 100);

        });
        session.close();
    }

    public List<DataPoint> getDataPoints(int sensorId) {
        Statement stmt = select().all().from("sensor_data").where(eq("sensor_id", sensorId));
        Session session = (Session) timelineStore.getConnection();

        List<DataPoint> output = new ArrayList<DataPoint>();
        ResultSet result = session.execute(stmt);
        for (Row row : result) {
            output.add(
                    new DataPoint(
                            row.getInt("sensor_id"),
                            row.getLong("timestamp"),
                            row.getBytes("data").array()));
        }

        session.close();
        return output;
    }

}
