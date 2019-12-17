import static org.junit.Assert.*;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * @author Yuxin Fan
 * @create 2019-12-16
 */
public class HBaseUtilTest {

  @Test
  public void createTable() {
    assertTrue(HBaseUtil.createTable("FileTable", new String[]{"fileInfo", "saveInfo"}));
  }

  @Test
  public void putRow() {
    assertTrue(HBaseUtil.putRow("FileTable", "rowKey1", "fileInfo", "name", "file1.txt"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowKey1", "fileInfo", "type", "txt"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowKey1", "fileInfo", "size", "1024"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowKey1", "saveInfo", "creator", "faushine"));

    assertTrue(HBaseUtil.putRow("FileTable", "rowKey2", "fileInfo", "name", "file2.txt"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowKey2", "fileInfo", "type", "txt"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowKey2", "fileInfo", "size", "1024"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowKey2", "saveInfo", "creator", "faushine"));

  }

  @Test
  public void getRow() {
    Result result = HBaseUtil.getRow("FileTable", "rowKey1");
    if (result != null) {
      assertEquals("rowKey1",Bytes.toString(result.getRow()));
      assertEquals("file1.txt",Bytes
          .toString(result.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));
    }
  }

  @Test
  public void getScanner() {
    ResultScanner scanner = HBaseUtil.getScanner("FileTable","rowKey1","rowKey2");
    if (scanner!=null){
      scanner.forEach(result -> {
        assertEquals("rowKey1",Bytes.toString(result.getRow()));
        assertEquals("file1.txt",Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"),Bytes.toBytes("name"))));
      });
    }
  }

  @Test
  public void deleteRow() {
    assertTrue(HBaseUtil.deleteRow("FileTable","rowKey1"));
  }

  @Test
  public void deleteTable() {
    assertTrue(HBaseUtil.deleteTable("FileTable"));
  }
}