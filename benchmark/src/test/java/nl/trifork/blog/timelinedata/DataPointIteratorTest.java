package nl.trifork.blog.timelinedata;

import com.google.common.collect.Lists;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;


public class DataPointIteratorTest {

    @Test
    public void test(){
        DataPointIterator dataPointIterator = new DataPointIterator(2, 50, 3, 0L);
        List<DataPoint> dataPoints = Lists.newArrayList(dataPointIterator);
        assertThat(dataPoints, contains(
                dataPointMatcher(0, 0, 50),
                dataPointMatcher(1, 0, 50),
                dataPointMatcher(0, 1, 50),
                dataPointMatcher(1, 1, 50),
                dataPointMatcher(0, 2, 50),
                dataPointMatcher(1, 2, 50)
        ));
    }


    private DataPointMatcher dataPointMatcher(int sensorId, long timestamp, int dataLength){
        return new DataPointMatcher(sensorId, timestamp, dataLength);
    }

    private static class DataPointMatcher extends BaseMatcher<DataPoint>  {

        private int sensorId;
        private long timestamp;
        private int dataLength;

        public DataPointMatcher(int sensorId, long timestamp, int dataLength) {
            this.sensorId = sensorId;
            this.timestamp = timestamp;
            this.dataLength = dataLength;
        }

        @Override
        public boolean matches(Object item) {
            DataPoint dataPoint = (DataPoint) item;
            return dataPoint.getSensorId() == this.sensorId &&
                    dataPoint.getTimestamp() == this.timestamp &&
                    dataPoint.getData().length == this.dataLength;
        }

        @Override
        public void describeTo(Description description) {

        }
    }

}