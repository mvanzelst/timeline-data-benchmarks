package nl.trifork.blog.timelinedata.store;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class MongoDbTimelineStore implements TimelineStore {

    public void initSchema() {
        MongoDatabase mongoDatabase = (MongoDatabase) getConnection();
        if(mongoDatabase.getCollection("time_series") != null){
            mongoDatabase.getCollection("time_series").drop();
        }
        mongoDatabase.createCollection("time_series");
        MongoCollection<Document> time_series = mongoDatabase.getCollection("time_series");
        time_series.createIndex(new Document("sensorId", 1).append("timestamp", 1));
    }

    public Object getConnection() {
        MongoClient mongoClient = new MongoClient("172.17.0.1");
        return mongoClient.getDatabase("time_series_db");
    }

}
