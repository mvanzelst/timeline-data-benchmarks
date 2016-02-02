package nl.trifork.blog.timelinedata.store;

public interface TimelineStore {

    void initSchema();

    Object getConnection();
}
