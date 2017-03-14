package example.hive.serde;

import example.hive.serde.ux.UXClickEvent;
import example.hive.serde.ux.UXLogGenericRecordReader;
import example.hive.serde.ux.UXLogGenericRecordWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Created by jihun.jo on 2017-03-14.
 */
public class UXKpiSerdeTest {
  private static final String FILENAME = "test_ux_data.log";

  @Test
  public void deserialize() throws Exception {
    JobConf job = new JobConf();
    String[] host = new String[1]; // empty
    FileSplit split = new FileSplit(new Path(getClass().getClassLoader().getResource(FILENAME).getFile()), 0, 1024*1024, host);
    UXLogGenericRecordReader rr = new UXLogGenericRecordReader(job, split, null);
    UXLogGenericRecordWritable record = new UXLogGenericRecordWritable();

    boolean isGetRecord = rr.next(null, record);
    assertEquals(true, isGetRecord);
    UXClickEvent event = (UXClickEvent)record.getRecord();
    assertEquals(1489476444211l,event.timestamp);
    assertEquals("START",event.target);

    Properties tbl = new Properties();
    tbl.setProperty(Names.SCHEMA_NAME, "click");
    tbl.setProperty(serdeConstants.LIST_COLUMNS,"millisecond,pos_x,pos_y,target");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES,"bigint,int,int,string");
    UXKpiSerde instance = new UXKpiSerde();
    instance.initialize(null, tbl);
    List<Object> rs = (List<Object>) instance.deserialize(record);
    assertEquals(1489476444211l, rs.get(0));
    assertEquals("START", rs.get(3));
  }

  @Test
  public void deserializeRows() throws Exception {
    JobConf job = new JobConf();
    String[] host = new String[1]; // empty
    FileSplit split = new FileSplit(new Path(getClass().getClassLoader().getResource(FILENAME).getFile()), 0, 1024*1024, host);
    UXLogGenericRecordReader rr = new UXLogGenericRecordReader(job, split, null);
    UXLogGenericRecordWritable record = new UXLogGenericRecordWritable();

    assertEquals(true, rr.next(null, record));
    assertEquals(1489476444211l,((UXClickEvent)record.getRecord()).timestamp);
    assertEquals(true, rr.next(null, record));
    assertEquals(1489476344211l,((UXClickEvent)record.getRecord()).timestamp);
    assertEquals(true, rr.next(null, record));
    assertEquals(1489486344211l,((UXClickEvent)record.getRecord()).timestamp);
    assertEquals(true, rr.next(null, record));
    assertEquals(1489496344211l,((UXClickEvent)record.getRecord()).timestamp);
    assertEquals(true, rr.next(null, record));
    assertEquals(1489476444211l,((UXClickEvent)record.getRecord()).timestamp);
  }

  @Test
  public void deserializeLast() throws Exception {
    JobConf job = new JobConf();
    String[] host = new String[1]; // empty
    FileSplit split = new FileSplit(new Path(getClass().getClassLoader().getResource(FILENAME).getFile()), 100, 1024*1024, host);
    UXLogGenericRecordReader rr = new UXLogGenericRecordReader(job, split, null);
    UXLogGenericRecordWritable record = new UXLogGenericRecordWritable();

    assertEquals(true, rr.next(null, record));
    assertEquals(1489476544211l,((UXClickEvent)record.getRecord()).timestamp);
    assertEquals(true, rr.next(null, record));
    assertEquals(1489475344211l,((UXClickEvent)record.getRecord()).timestamp);
    assertEquals(false, rr.next(null, record));
  }
}
