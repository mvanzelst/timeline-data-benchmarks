package nl.trifork.blog.timelinedata.storage;

public interface TimelineStore {

    public void initSchema();

    public Object getConnection();
}
