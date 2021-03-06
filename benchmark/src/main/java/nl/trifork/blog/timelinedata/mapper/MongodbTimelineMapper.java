package nl.trifork.blog.timelinedata.mapper;

import com.google.common.collect.Iterators;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import nl.trifork.blog.timelinedata.DataPoint;
import nl.trifork.blog.timelinedata.DataPointIterator;
import nl.trifork.blog.timelinedata.store.MongoDbTimelineStore;
import nl.trifork.blog.timelinedata.store.TimelineStore;
import org.bson.Document;
import org.bson.types.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MongodbTimelineMapper implements TimelineMapper {

    private static final Logger logger = LoggerFactory.getLogger(TimelineStore.class);

    private final int batchSize = 1000;

    private final MongoDbTimelineStore timelineStore;

    public MongodbTimelineMapper(boolean initSchema) {
        timelineStore = new MongoDbTimelineStore();
        if(initSchema) {
            timelineStore.initSchema();
        }
    }

    public void storeDataPoints(DataPointIterator dataPointIterator) {
        MongoDatabase db = (MongoDatabase) timelineStore.getConnection();
        MongoCollection<Document> timeSeriesCollection = db.getCollection("time_series");
        timeSeriesCollection.withWriteConcern(WriteConcern.JOURNALED);
        AtomicInteger atomicInteger = new AtomicInteger();
        Iterators.partition(dataPointIterator, batchSize)
                .forEachRemaining(dataPointBatch -> {
                    
                    List<Document> documents = dataPointBatch.stream().map(dataPoint -> {
                        Document document = new Document();
                        document.append("_id", String.format("%s-%s", dataPoint.getSensorId(), dataPoint.getTimestamp()));
                        document.append("sensorId", dataPoint.getSensorId());
                        document.append("data", dataPoint.getData());
                        document.append("timestamp", dataPoint.getTimestamp());
                        return document;
                    }).collect(Collectors.toList());

                    atomicInteger.addAndGet(documents.size());

                    timeSeriesCollection.insertMany(documents);

                    logger.info("Inserted {} records out of {} - {}%",
                            atomicInteger.get(), dataPointIterator.size(),
                            ((double) atomicInteger.get() / dataPointIterator.size()) * 100);
                });
    }

    @Override
    public List<DataPoint> getDataPoints(int sensorId) {
        MongoDatabase db = (MongoDatabase) timelineStore.getConnection();
        FindIterable<Document> documents = db.getCollection("time_series").find(new Document("sensorId", sensorId));
        List<DataPoint> output = new ArrayList<>();
        for (Document document : documents) {
            output.add(new DataPoint(
                    document.getInteger("sensorId"),
                    document.getLong("timestamp"),
                    document.get("data", Binary.class).getData()));
        }
        return output;
    }

}
