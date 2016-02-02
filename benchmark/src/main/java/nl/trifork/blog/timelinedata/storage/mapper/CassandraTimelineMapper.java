package nl.trifork.blog.timelinedata.storage.mapper;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.QueryTimeoutException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;
import nl.trifork.blog.timelinedata.model.DataPoint;
import nl.trifork.blog.timelinedata.storage.CassandraTimelineStore;
import nl.trifork.blog.timelinedata.storage.TimelineStore;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CassandraTimelineMapper implements TimelineMapper {

    private TimelineStore timelineStore;

    public CassandraTimelineMapper() {
        timelineStore = new CassandraTimelineStore();
        timelineStore.initSchema();
    }

    public void storeDataPoints(List<DataPoint> datapoints) {
        while (true) {
            Session session = (Session) timelineStore.getConnection();
            try {

                PreparedStatement ps = session.prepare("INSERT INTO sensor_data (sensor_id, timestamp, data) VALUES (?, ?, ?)");
                BatchStatement batch = new BatchStatement();
                for (DataPoint dataPoint : datapoints) {
                    batch.add(ps.bind(dataPoint.sensorId, dataPoint.timestamp, ByteBuffer.wrap(dataPoint.data)));
                }

                session.execute(batch);
                session.close();
            } catch (QueryTimeoutException e) {
                System.err.println(ExceptionUtils.getStackTrace(e));
                // Retry
                continue;
            } finally {
                session.close();
            }
            break;
        }
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
