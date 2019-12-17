import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author Yuxin Fan
 * @create 2019-12-16
 */
public class HBaseFilterTest {

  @Test
  public void createTableTest() {
    assertTrue(HBaseUtil.createTable("FileTable", new String[]{"fileInfo", "saveInfo"}));
  }

  @Test
  public void putRowTest() {
    assertTrue(HBaseUtil.putRow("FileTable", "rowKey1", "fileInfo", "name", "file1.txt"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowKey1", "fileInfo", "type", "txt"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowKey1", "fileInfo", "size", "1024"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowKey1", "saveInfo", "creator", "faushine"));

    assertTrue(HBaseUtil.putRow("FileTable", "rowKey2", "fileInfo", "name", "file2.txt"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowKey2", "fileInfo", "type", "txt"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowKey2", "fileInfo", "size", "1024"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowKey2", "saveInfo", "creator", "faushine"));

    assertTrue(HBaseUtil.putRow("FileTable", "rowkey3", "fileInfo", "name", "file3.pdf"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowkey3", "fileInfo", "type", "pdf"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowkey3", "fileInfo", "size", "2048"));
    assertTrue(HBaseUtil.putRow("FileTable", "rowkey3", "fileInfo", "creator", "faushine"));
  }

  @Test
  public void rowFilterTest() {
    Filter filter = new RowFilter(CompareOp.NO_OP.EQUAL,
        new BinaryComparator(Bytes.toBytes("rowKey1")));
    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(filter));
    ResultScanner resultScanner = HBaseUtil
        .getScanner("FileTable", "rowKey1", "rowKey3", filterList);
    if (resultScanner != null) {
      resultScanner.forEach(result -> {
        assertEquals("rowKey1", Bytes.toString(result.getRow()));
        assertEquals("file1.txt",
            Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));
      });
    }
    resultScanner.close();
  }

  @Test
  public void prefixFilterTest() {
    Filter filter = new PrefixFilter(Bytes.toBytes("rowKey2"));
    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(filter));
    ResultScanner scanner = HBaseUtil.getScanner("FileTable", "rowKey1", "rowKey3", filterList);

    if (scanner != null) {
      scanner.forEach(result -> {
        assertEquals("rowKey2", Bytes.toString(result.getRow()));
        assertEquals("file2.txt",
            Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));
      });
      scanner.close();
    }
  }

  @Test
  public void keyOnlyFilterTest() {
    Filter filter = new KeyOnlyFilter(true);
    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(filter));
    ResultScanner scanner = HBaseUtil.getScanner("FileTable", "rowKey1", "rowKey3", filterList);

    if (scanner != null) {
      scanner.forEach(result -> {
        System.out.println(Bytes.toString(result.getRow()));
        System.out.println(
            Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));
      });
      scanner.close();
    }
  }

  @Test
  public void columnPrefixFilterTest() {
    Filter filter = new ColumnPrefixFilter(Bytes.toBytes("nam"));
    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(filter));
    ResultScanner scanner = HBaseUtil.getScanner("FileTable", "rowKey1", "rowKey3", filterList);

    if (scanner != null) {
      scanner.forEach(result -> {
        System.out.println(Bytes.toString(result.getRow()));
        System.out.println(
            Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));
      });
      scanner.close();
    }
  }
}
