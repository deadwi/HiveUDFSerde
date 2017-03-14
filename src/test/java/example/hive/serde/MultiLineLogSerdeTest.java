package example.hive.serde;

import example.hive.serde.multiline.MultiLineLogGenericRecordReader;
import example.hive.serde.multiline.MultiLineLogGenericRecordWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Created by jihun.jo on 2017-03-13.
 */
public class MultiLineLogSerdeTest {
  @Test
  public void deserialize() throws Exception {
    JobConf job = new JobConf();
    job.set(Names.TABLE_NAME, "battle");

    String[] host = new String[1]; // empty
    FileSplit split = new FileSplit(new Path(getClass().getClassLoader().getResource("test_battle_data.log").getFile()), 0, 1024*1024, host);
    MultiLineLogGenericRecordReader rr = new MultiLineLogGenericRecordReader(job, split, null);
    MultiLineLogGenericRecordWritable record = new MultiLineLogGenericRecordWritable();

    boolean isGetRecord = rr.next(null, record);
    assertEquals(true, isGetRecord);
    Map<String, String> dataSet = (Map<String, String>)record.getRecord();
    assertEquals("20170301132100",dataSet.get("datetime"));
    assertEquals("g1003",dataSet.get("gameid"));
    assertEquals("120",dataSet.get("playtime"));

    Properties tbl = new Properties();
    tbl.setProperty(Names.SCHEMA_NAME, "battle");
    tbl.setProperty(serdeConstants.LIST_COLUMNS,"datetime,gameid,playtime,score");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES,"string,string,int,int");
    MultiLineLogSerde instance = new MultiLineLogSerde();
    instance.initialize(null, tbl);
    List<Object> rs = (List<Object>) instance.deserialize(record);
    assertEquals("20170301132100", rs.get(0));
    assertEquals("g1003", rs.get(1));
    assertEquals(120, rs.get(2));
  }
}