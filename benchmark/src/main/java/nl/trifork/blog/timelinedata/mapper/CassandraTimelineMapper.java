package nl.trifork.blog.timelinedata.mapper;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.collect.Iterators;
import nl.trifork.blog.timelinedata.DataPoint;
import nl.trifork.blog.timelinedata.store.CassandraTimelineStore;
import nl.trifork.blog.timelinedata.store.TimelineStore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CassandraTimelineMapper implements TimelineMapper {

    private final int batchSize = 25;

    private TimelineStore timelineStore;

    public CassandraTimelineMapper() {
        timelineStore = new CassandraTimelineStore();
        timelineStore.initSchema();
    }

    public void storeDataPoints(Iterator<DataPoint> dataPoints) {
        Session session = (Session) timelineStore.getConnection();
        PreparedStatement ps = session.prepare(
                "INSERT INTO sensor_data (sensor_id, timestamp, data) VALUES (?, ?, ?)");
        Iterators.partition(dataPoints, batchSize)
                .forEachRemaining(dataPointBatch -> {
                    BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
                    for (DataPoint dataPoint : dataPointBatch) {
                        batch.add(ps.bind(dataPoint.sensorId, dataPoint.timestamp, ByteBuffer.wrap(dataPoint.data)));
                    }
                    session.execute(batch);
                });
        session.close();
    }

    public List<DataPoint> getDataPoints(String sensorId, long startTimestamp, long endTimestamp, int limit) {
        Where where = QueryBuilder.select().all().from("sensor_data").where();

        where.and(QueryBuilder.eq("sensor_id", sensorId));

        if (startTimestamp > -1)
            where.and(QueryBuilder.gte("timestamp", startTimestamp));

        if (endTimestamp > -1)
            where.and(QueryBuilder.lt("timestamp", endTimestamp));

        if (limit > -1)
            where.limit(limit);

        Statement stmt = where;

        Session session = (Session) timelineStore.getConnection();

        List<DataPoint> output = new ArrayList<DataPoint>();
        ResultSet result = session.execute(stmt);
        for (Row row : result) {
            output.add(
                    new DataPoint(
                            row.getString("sensor_id"),
                            row.getLong("timestamp"),
                            row.getBytes("data").array()
                    )
            );
        }

        session.close();
        return output;
    }

}
