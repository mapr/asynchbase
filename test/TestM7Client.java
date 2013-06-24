package org.hbase.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import com.mapr.fs.ClusterConf.ClusterEntry;
import com.mapr.fs.MapRFileSystem;
import com.mapr.fs.proto.Dbserver.ColumnFamilyAttr;
import com.mapr.fs.proto.Dbserver.SchemaFamily;

public class TestM7Client {
  private static final Logger LOG = Common.logger(TestM7Client.class);

  private static final String TABLE_NAME_STR = "/~~async_unittest_tmptable~~";
  private static final Path   TABLE_PATH = new Path(TABLE_NAME_STR);
  private static final byte[] TABLE_NAME = TABLE_NAME_STR.getBytes();

  private static final String FAMILY_NAME_S = "info";
  private static final byte[] FAMILY_NAME = FAMILY_NAME_S.getBytes();

  private static final int    ROW_COUNT = 50;
  private static final int    VALUE_SIZE = 10;
  private static final int    MAX_COL = 5;

  private static MapRFileSystem maprfs = null;

  private static final HBaseClient dbclient = new HBaseClient("localhost");

  private final Random random = new Random();

  private final Map<String, KeyValue> insertedKeyValueMap = new LinkedHashMap<String, KeyValue>();

  private static boolean initialized = false;

  @BeforeClass
  public static void initTestEnvironment() throws Exception {
    String mapr_home = System.getenv("MAPR_HOME");
    if (mapr_home == null || !new File(mapr_home).exists()) {
      throw new RuntimeException("${MAPR_HOME} is not set.");
    }
    if (!new File(mapr_home, "conf/mapr-clusters.conf").exists()) {
      throw new RuntimeException(mapr_home+"/conf/mapr-clusters.conf does not exist.");
    }

    ClusterEntry cluster = MapRFileSystem.getClusterConf().getClusterList().get(0);
    LOG.info(String.format("Connected to cluster '%s' at %s:%d", 
                            cluster.getClusterName(),
                            cluster.getIpList().get(0).getAddr(),
                            cluster.getIpList().get(0).getPort()));

    maprfs = (MapRFileSystem) FileSystem.get(new URI("maprfs:///"), new Configuration());
    if (maprfs.exists(TABLE_PATH)) {
      LOG.info("Table exists, deleteing.");
      maprfs.delete(TABLE_PATH);
    }
    maprfs.createTable(TABLE_PATH);
    maprfs.createColumnFamily(TABLE_PATH, FAMILY_NAME_S, ColumnFamilyAttr.newBuilder()
                                                         .setSchFamily(SchemaFamily
                                                                       .newBuilder().build())
                                                         .build());
    initialized = true;
  }

  @AfterClass
  public static void cleanupTestEnvironment() throws Exception {
    if (initialized) {
      dbclient.shutdown().joinUninterruptibly();
      if (maprfs.exists(TABLE_PATH)) {
        maprfs.delete(TABLE_PATH);
        LOG.info("Table deleted.");
      }
    }
  }

  @Test
  public void testAllInOne() throws Exception {
    // let's start with putting some rows
    try {
      insertedKeyValueMap.clear();
      int keyIndex = 0;
      for (int i = 0; i < ROW_COUNT; i++) {
        keyIndex += 10 + random.nextInt(100);
        String key = getKey(keyIndex);
        final KeyValue kv = new KeyValue(key.getBytes(), FAMILY_NAME, getCol(i % MAX_COL)
            .getBytes(), getRandomValue());
        final PutRequest put = new PutRequest(TABLE_NAME, kv);
        LOG.info("Putting key=" + key);
        dbclient.put(put);
        insertedKeyValueMap.put(key, kv);
      }
      LOG.info("Flushing puts");
      dbclient.flush().joinUninterruptibly();
    } catch (Throwable t) {
      LOG.error("Put failed", t);
      fail();
    }

    // now let's get those rows back
    try {
      for (String rowkey : insertedKeyValueMap.keySet()) {
        final GetRequest get = new GetRequest(TABLE_NAME_STR, rowkey);
        LOG.info("Getting key=" + rowkey);
        KeyValue get_kv = dbclient.get(get).joinUninterruptibly().get(0);
        KeyValue put_kv = insertedKeyValueMap.get(rowkey);
        put_kv = new KeyValue(put_kv.key(), put_kv.family(), put_kv.qualifier(),
            get_kv.timestamp(), put_kv.value());
        assertEquals("Comparing KeyValue", get_kv, put_kv);
      }
    } catch (Throwable t) {
      LOG.error("Get failed", t);
      fail();
    }

    // how about a scan over the entire table?
    final Scanner scanner = dbclient.newScanner(TABLE_NAME_STR);
    LOG.info("Start scanner=" + scanner);
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (ArrayList<KeyValue> arrayList : rows) {
          LOG.info("Scanned row=" + arrayList.get(0));
        }
      }
    } catch (Throwable t) {
      LOG.error("Scan failed", t);
      fail();
    }

    // time for a partial scan
    runPartialScan(2, 7);

    // It's crazy time
    ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(4, 4, 60, TimeUnit.SECONDS,
      new LinkedBlockingQueue<Runnable>());
    for (int i = 0; i < 1000; i++) {
      poolExecutor.execute(new Runnable() {
        @Override
        public void run() {
          int start = random.nextInt(ROW_COUNT);
          runPartialScan(start, start + random.nextInt(ROW_COUNT-start));
        }
      });
    }
    poolExecutor.shutdown();
    poolExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

    // lastly, let's cleanup after ourself
    try {
      for (KeyValue kv : insertedKeyValueMap.values()) {
        final DeleteRequest delete = new DeleteRequest(TABLE_NAME, kv);
        LOG.info("Deleting row=" + kv);
        dbclient.delete(delete);
      }
      dbclient.flush().joinUninterruptibly();
    } catch (Throwable t) {
      LOG.error("Delete failed", t);
      fail();
    }
  }

  private void runPartialScan(int start, int end) {
    final Deque<String> rowKeys = new LinkedList<String>();
    final Scanner partial_scanner = dbclient.newScanner(TABLE_NAME_STR);
    Iterator<String> keys = insertedKeyValueMap.keySet().iterator();
    for (int j = 0; j <= end; j++) {
      if (j < start) keys.next();
      else rowKeys.add(keys.next());
    }
    partial_scanner.setStartKey(rowKeys.getFirst());
    partial_scanner.setStopKey(rowKeys.getLast());
    LOG.info("[" + Thread.currentThread().getName()+"] - Start scanner=" + partial_scanner);
    try {
      ArrayList<ArrayList<KeyValue>> rows = null;
      while ((rows = partial_scanner.nextRows().joinUninterruptibly()) != null) {
        for (ArrayList<KeyValue> arrayList : rows) {
          KeyValue row = arrayList.get(0);
          LOG.info("[" + Thread.currentThread().getName()+"] - Scanned row=" + row);
          assertEquals(rowKeys.pop(), new String(row.key()));
        }
      }
      LOG.info("[" + Thread.currentThread().getName()+"] - Scan complete");
    } catch (Throwable t) {
      LOG.error("Scan failed", t);
      fail();
    }
  }

  private String getKey(int i) {
    return String.format("testrow%08d", i);
  }

  private String getCol(int i) {
    return String.format("testcol%08d", i);
  }

  private byte[] getRandomValue() {
    byte[] bytes;
    random.nextBytes(bytes = new byte[VALUE_SIZE]);
    return bytes;
  }
}
