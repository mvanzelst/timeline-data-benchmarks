package nl.trifork.blog.timelinedata.mapper;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.google.common.collect.Iterators;
import nl.trifork.blog.timelinedata.DataPoint;
import nl.trifork.blog.timelinedata.DataPointIterator;
import nl.trifork.blog.timelinedata.store.CassandraTimelineStore;
import nl.trifork.blog.timelinedata.store.TimelineStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

public class CassandraTimelineMapper implements TimelineMapper {

    private static final Logger logger = LoggerFactory.getLogger(TimelineStore.class);

    private final int batchSize = 40;
    private final RetryTemplate retryTemplate;

    private TimelineStore timelineStore;

    public CassandraTimelineMapper() {
        timelineStore = new CassandraTimelineStore();
        timelineStore.initSchema();

        Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
        retryableExceptions.put(QueryExecutionException.class, true);
        retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(new SimpleRetryPolicy(5, retryableExceptions));
    }

    public void storeDataPoints(DataPointIterator dataPointIterator) {
        Session session = (Session) timelineStore.getConnection();
        PreparedStatement ps = session.prepare(
                "INSERT INTO sensor_data (sensor_id, timestamp, data) VALUES (?, ?, ?)");


        AtomicInteger atomicInteger = new AtomicInteger();
        Iterators.partition(dataPointIterator, batchSize)
                .forEachRemaining(dataPointBatch -> {
                    BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
                    for (DataPoint dataPoint : dataPointBatch) {
                        batch.add(ps.bind(dataPoint.sensorId, dataPoint.timestamp, ByteBuffer.wrap(dataPoint.data)));
                    }
                    atomicInteger.getAndAdd(dataPointBatch.size());
                    retryTemplate.execute((retryContext) -> session.execute(batch));
                    logger.info("Inserted {} records out of {} - {}%",
                            atomicInteger.get(), dataPointIterator.size(),
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
