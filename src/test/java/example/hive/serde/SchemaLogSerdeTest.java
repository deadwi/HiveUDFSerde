package example.hive.serde;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Created by jihun.jo on 2017-03-09.
 */
public class SchemaLogSerdeTest {
  @Test
  public void deserialize() throws Exception {
    SchemaLogSerde instance = new SchemaLogSerde();
    Properties tbl = new Properties();
    tbl.setProperty(Names.SCHEMA_NAME, "login");
    tbl.setProperty(serdeConstants.LIST_COLUMNS,"datetime,schema,version,level,money");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES,"string,string,string,int,bigint");
    instance.initialize(null, tbl);
    List<Object> rs = (List<Object>) instance.deserialize(new Text("2013-08-19 22:19:27,login,v1,3,1000"));

    assertEquals(5, rs.size());
    assertEquals("login", rs.get(1));
    assertEquals(3, rs.get(3));
    assertEquals(1000l, rs.get(4));
  }

  @Test
  public void deserializePreviousVersion() throws Exception {
    SchemaLogSerde instance = new SchemaLogSerde();
    Properties tbl = new Properties();
    tbl.setProperty(Names.SCHEMA_NAME, "login");
    tbl.setProperty(serdeConstants.LIST_COLUMNS,"datetime,schema,version,level,exp,money");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES,"string,string,string,int,bigint,bigint");
    instance.initialize(null, tbl);
    List<Object> rs = (List<Object>) instance.deserialize(new Text("2013-08-19 22:19:27,login,v2,3,300,1000"));

    assertEquals(6, rs.size());
    assertEquals(300l, rs.get(4));

    rs = (List<Object>) instance.deserialize(new Text("2013-08-19 22:19:27,login,v1,1000"));
    assertEquals(6, rs.size());
    assertEquals(null, rs.get(4));
  }
}