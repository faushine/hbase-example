import static org.junit.Assert.*;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Yuxin Fan
 * @create 2019-12-16
 */
public class HBaseConnTest {

  @Test
  public void getHBaseConn() {
    Connection connection = HBaseConn.getHBaseConn();
    assertFalse(connection.isClosed());
    HBaseConn.closeConn();
    assertTrue(connection.isClosed());
  }

  @Test
  public void getTable() {
    try{
      Table table = HBaseConn.getTable("US_POPULATION");
      assertEquals(table.getName().getNameAsString(),"US_POPULATION");
      table.close();
    }catch (IOException e){
      e.printStackTrace();
    }
  }
}