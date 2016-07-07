/* Copyright (c) 2012 & onwards. MapR Tech, Inc., All rights reserved */
package org.hbase.async;

import java.lang.Thread;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mapr.fs.MapRHTable;
import com.mapr.fs.MapRResultScanner;
import com.mapr.fs.jni.MapRConstants.PutConstants;
import com.mapr.fs.jni.MapRGet;
import com.mapr.fs.jni.MapRIncrement;
import com.mapr.fs.jni.MapRPut;
import com.mapr.fs.jni.MapRResult;
import com.mapr.fs.jni.MapRScan;
import com.stumbleupon.async.Deferred;

public class MapRThreadPool implements com.mapr.fs.jni.MapRCallBackQueue {
  private static final Logger LOG = LoggerFactory.getLogger(MapRThreadPool.class);

  static class AsyncHBaseRpc {
    HBaseRpc rpc;
    MapRHTable mTable;
    Deferred<?> deferred;
    Scanner scanner; // Used only by closeScanner()

    public AsyncHBaseRpc(HBaseRpc rpc, MapRHTable mTable) {
      this.rpc = rpc;
      this.mTable = mTable;
      this.deferred = null;
      this.scanner = null;
    }

    public AsyncHBaseRpc(HBaseRpc rpc, MapRHTable mTable, Deferred<?> deferred){
      this(rpc, mTable);
      this.deferred = deferred;
    }

    public AsyncHBaseRpc(HBaseRpc rpc, MapRHTable mTable, Deferred<?> deferred,
        Scanner scanner) {
      this(rpc, mTable, deferred);
      this.scanner = scanner;
    }
  }

  // pool for all rpcs except scanner
  public static final int DEFAULT_WORKER_THREADS = 128;
  public static final int DEFAULT_CALLBACK_THREADS = 5;
  private ExecutorService pool;
  BlockingQueue<AsyncHBaseRpc> asyncrpcQueue;

  // Separate pool for scanner
  public static final int MIN_SCAN_THREADS = 3;
  public static final int MAX_SCAN_THREADS = 32;
  private ExecutorService scanpool;
  BlockingQueue<MapRScanPlus> scanRequestQueue;

  // Used by scan
  static class MapRScanPlus {
    MapRScan mscan;
    HBaseRpc dummyRpc;
    Scanner scan;
    MapRHTable mtbl;

    MapRScanPlus(MapRScan mscan, HBaseRpc dummyRpc, Scanner scan,
        MapRHTable mt) {
      this.mscan = mscan;
      this.dummyRpc = dummyRpc;
      this.scan = scan;
      this.mtbl = mt;
    }
  }

  MapRThreadPool(Configuration conf) {
    asyncrpcQueue = new LinkedBlockingQueue<AsyncHBaseRpc> ();

    // Taken from Java doc:
    // A ThreadPoolExecutor will automatically adjust the pool size according
    // to the bounds set by corePoolSize and maximumPoolSize. When a new task
    // is submitted in method execute(java.lang.Runnable), and fewer than
    // corePoolSize threads are running, a new thread is created to handle the
    // request, even if other worker threads are idle. If there are more than
    // corePoolSize but less than maximumPoolSize threads running, a new thread
    // will be created only if the queue is full. By setting maximumPoolSize 
    // to an essentially unbounded value such as Integer.MAX_VALUE, you
    // allow the pool to accommodate an arbitrary number of concurrent tasks.
    int workerThreads = conf.getInt("fs.mapr.async.worker.threads", DEFAULT_WORKER_THREADS);
    int callbackThreads = conf.getInt("fs.mapr.async.callback.threads", DEFAULT_CALLBACK_THREADS);
    LOG.info("Creating ThreadPoolExecutor with {} workerThreads and {} callbackThreads.", workerThreads, callbackThreads);
    int totalThreads = workerThreads + callbackThreads;
    pool = new ThreadPoolExecutor(
                  totalThreads, // core thread pool size,
                  totalThreads, // maximum thread pool size
                  1, // time to wait before reducing threads, if more then coreSz
                  TimeUnit.HOURS,
                  new LinkedBlockingQueue<Runnable>(),
                  new ThreadFactoryBuilder().setNameFormat("MapRDB-Main-%d").build(),
                  new ThreadPoolExecutor.CallerRunsPolicy());
    // Start minimum number of threads in pool.
    for (int i = 0; i < workerThreads; i++) {
      pool.execute(new RpcRunnable());
    }

    // All the threads in the scanpool will work on the scan requests in this
    // queue. If the max threads are hit, then they block for a thread to get
    // done and pick them up.
    this.scanRequestQueue = new LinkedBlockingQueue<MapRScanPlus>();

    // Separate threadpool for Scanner.
    scanpool = new ThreadPoolExecutor(
                  MIN_SCAN_THREADS, // core thread pool size
                  MAX_SCAN_THREADS, // maximum thread pool size
                  1,// time to wait before reducing threads, if more then coreSz
                  TimeUnit.HOURS,
                  new LinkedBlockingQueue<Runnable>(),
                  new ThreadFactoryBuilder().setNameFormat("MapRDB-Scan-%d").build(),
                  new ThreadPoolExecutor.CallerRunsPolicy());
    // Start minimum number of threads in pool.
    for (int i = 0; i < MIN_SCAN_THREADS; i++) {
      scanpool.execute(new ScanRpcRunnable(this));
    }
  }

  public boolean hasRemainingCapacity() {
    if (scanRequestQueue.remainingCapacity() != 0) {
      return true;
    }

    return false;
  }

  public void addToQ(MapRScanPlus mscanPlus) {
    scanRequestQueue.add(mscanPlus);
  }

  public MapRScanPlus takeFromQ() {
    MapRScanPlus msp;

    try {
      msp = scanRequestQueue.take();
    } catch (InterruptedException e) {
      msp = null;
    }

    return msp;
  }

  // Used by most rpcs
  public void sendRpc(HBaseRpc rpc, MapRHTable mTable) {
    AsyncHBaseRpc asrpc = new AsyncHBaseRpc(rpc, mTable);
    asyncrpcQueue.add(asrpc);
  }

  // Used only by flush()
  public void doFlush(Deferred<?> deferred, MapRHTable mTable) {
    AsyncHBaseRpc asrpc = new AsyncHBaseRpc(null, mTable, deferred);
    asyncrpcQueue.add(asrpc);
  }

  public void closeScanner(Deferred<?> d, MapRHTable mTable, Scanner scan) {
    AsyncHBaseRpc asrpc = new AsyncHBaseRpc(null, mTable, d, scan);
    asyncrpcQueue.add(asrpc);
  }

  // Used for callback/errback from async put
  public void runCallbackChain(LinkedList<Object> requests, LinkedList<Object> responses) {
    try {
      pool.execute(new CallBackRunnable(requests, responses));
    } catch(Throwable e) {
      // we do not want the MapR-RPC threads to die, hence we catch all Throwable
      LOG.error("Exception in async runCallbackChain: " + e.getMessage(), e);
    }
  }

  public void shutdown() {
    pool.shutdownNow();
    scanpool.shutdownNow();
  }

  // Used by rpcs like get, automic-incr
  class RpcRunnable implements Runnable {

    public RpcRunnable() {
    }

    public void run() {
      // handle connection
      while (true) {
        AsyncHBaseRpc asrpc;
        try {
          asrpc = asyncrpcQueue.take();
        } catch (InterruptedException e) {
          break;
        } catch (Exception e) {
          Deferred.fromError(e);
          continue;
        }

        HBaseRpc rpc = asrpc.rpc;
        MapRHTable mTable = asrpc.mTable;
        Scanner sc = asrpc.scanner;
        if (sc != null) {
          sc.mresultScanner.close();
          asrpc.deferred.callback(null);
        }
        else if (rpc == null) {
          // Assume it is flush for now
          try {
            mTable.asyncFlush();
            asrpc.deferred.callback(null);
          } catch (Exception e) {
            asrpc.deferred.callback(e);
            Deferred.fromError(e);
          }
        } else if (rpc instanceof GetRequest) {
          try {
            GetRequest grpc = (GetRequest)rpc;
            MapRGet mGet = MapRConverter.toMapRGet(grpc, mTable);
            MapRResult result = mTable.get(mGet);

            ArrayList<KeyValue> kvArr =
               MapRConverter.toAsyncHBaseResult(result, grpc.key(), mTable);
            grpc.callback(kvArr);
          } catch (IllegalArgumentException ie) {

            LOG.error("Exception in async get(): " + ie.getMessage());
            final Exception e = new NoSuchColumnFamilyException("Invalid column family", rpc);
            rpc.callback(e);
            Deferred.fromError(e);
          } catch (Exception e) {

            LOG.error("Exception in async get(): " + e.getMessage());
            rpc.callback(e);
            Deferred.fromError(e);
            return;
          }
        /* --- disable java side non blocking inserts -------------------------
        } else if (rpc instanceof PutRequest) {
          try {
            PutRequest prpc = (PutRequest)rpc;
            MapRPut mPut = MapRConverter.toMapRPut(prpc, mTable,
                                                   Bytes.toString(prpc.family()),
                                                   null);
            mTable.syncPut(mPut);
            rpc.callback(null);
          } catch (Exception e) {
            LOG.error("Exception in async PutRequest: " + e.getMessage());
            rpc.callback(e);
            Deferred.fromError(e);
            return;
          }
        ----------------------------------------------------------------------*/
        } else if (rpc instanceof AtomicIncrementRequest) {
          try {
            AtomicIncrementRequest arpc = (AtomicIncrementRequest)rpc;
            MapRIncrement mincr =
              MapRConverter.toMapRIncrement(arpc.key(),
                                            arpc.family(),
                                            arpc.qualifier(),
                                            arpc.getAmount(),
                                            mTable);
            mTable.increment(mincr);
            arpc.callback(mincr.newValues[0]);
          } catch (Exception e) {
            LOG.error("Exception in async AtomicIncrementRequest: " + e.getMessage());
            rpc.callback(e);
            Deferred.fromError(e);
            return;
          }
        } else if (rpc instanceof AppendRequest) {
          try {
            AppendRequest arpc = (AppendRequest)rpc;
            MapRPut mput = MapRConverter.toMapRPut(arpc, mTable);
            mTable.append(mput, arpc.returnResult());

            if (arpc.returnResult()) {
              ArrayList<KeyValue> kvArr = new ArrayList<KeyValue>();
              kvArr = MapRConverter.toAsyncHBaseResult(
                                                    mput, arpc.key(), mTable);
              arpc.callback(kvArr);
            }

            arpc.callback(null);
          } catch (Exception e) {
            LOG.error("Exception in async append(): " + e.getMessage());
            rpc.callback(e);
            Deferred.fromError(e);
            return;
          }
        } else if (rpc instanceof CompareAndSetRequest) {
          try {
            int id = 0;
            // get family id

            CompareAndSetRequest crpc = (CompareAndSetRequest)rpc;
            boolean useCf = false;
            if (crpc.family() != null) {
              String family = Bytes.toString(crpc.family());
              if (!family.isEmpty()) {
                useCf = true;
                try {
                  id = mTable.getFamilyId(family);
                } catch (IOException ioe) {
                  throw new IllegalArgumentException("Invalid column family " +
                                                     family, ioe);
                }
              }
            }

            boolean res = false;
            if (crpc.value() == null) {
              // Special case: treat as delete
              MapRPut mput =  new MapRPut(crpc.key(), id, new byte[][] { crpc.qualifier() }, 
                                          null, crpc.timestamp(),
                                          PutConstants.TYPE_DELETE_CELLS_ASYNC);
              res = mTable.checkAndDelete(crpc.key(), useCf, id,
                                          crpc.qualifier(), crpc.expectedValue(),
                                          mput);
            } else {
              MapRPut mput =  new MapRPut(crpc.key(), id, new byte[][] { crpc.qualifier() }, 
                                          new byte[][] { crpc.value() }, 
                                          crpc.timestamp(), 
                                          PutConstants.TYPE_PUT_ROW_ASYNC);
              res = mTable.checkAndPut(crpc.key(), useCf, id,
                                       crpc.qualifier(), crpc.expectedValue(),
                                       mput);
            }

            crpc.callback(new Boolean(res));
          } catch (Exception e) {
            LOG.error("Exception in async CompareAndSetRequest: " + e.getMessage());
            rpc.callback(e);
            Deferred.fromError(e);
            return;
          }
        } else if (rpc instanceof DeleteRequest) {
          try {
            DeleteRequest drpc = (DeleteRequest)rpc;
            MapRPut mput = MapRConverter.toMapRPut(drpc, mTable);
            mTable.delete(mput);
            drpc.callback(null);
          } catch (Exception e) {
            LOG.error("Exception in async DeleteRequest: " + e.getMessage());
            rpc.callback(e);
            Deferred.fromError(e);
            return;
          }
        }
      }
    }
  }

  public void addRequest(HBaseRpc dummyRpc, Scanner scan, MapRThreadPool mpool){
    try {
      // NOTE: Do this only once per scan
      if (scan.mscan == null) {
        scan.mscan = MapRConverter.toMapRScan(scan, scan.mTable);
        if (scan.getFilterMsg() != null) {
          scan.mscan.setFilter(scan.getFilterMsg().toByteArray());
        }
      }

      // These cannot be in the constructor as client can change these
      // properties after creating object but before issuing scan.
      scan.mscan.startRow = scan.getCurrentKey();
      scan.mscan.stopRow = scan.getStopKey();

      MapRScanPlus mscanPlus = new MapRScanPlus(scan.mscan, dummyRpc, scan,
          scan.mTable);
      if (mpool.hasRemainingCapacity()) {
        mpool.addToQ(mscanPlus);
      } else {
        throw new UnknownScannerException("Failed to add scan request " +
            "to queue", dummyRpc);
      }
    } catch (Exception e) {
      LOG.error("Exception in addRequest: " + e.getMessage());
      dummyRpc.callback(e);
      Deferred.fromError(e);
    }
  }

  static class ScanRpcRunnable implements Runnable {
    MapRThreadPool mTpool;

    ScanRpcRunnable(MapRThreadPool mtp) {
      this.mTpool = mtp;
    }

    public void run() {
      while (true) {
        HBaseRpc dummyRpc = null;
        try {
          // handle connection
          MapRScanPlus mscanPlus = mTpool.takeFromQ();

          if (mscanPlus == null) {
            break;
          }

          MapRScan mscan = mscanPlus.mscan;
          Scanner scan = mscanPlus.scan;
          dummyRpc = mscanPlus.dummyRpc;
          MapRHTable mTable = mscanPlus.mtbl;

          // NOTE: Do this only once per scan
          if (scan.mresultScanner == null) {
            scan.mresultScanner = new MapRResultScanner(mscan, mTable);
          }

          // Do the scan
          MapRResult[] mresults =
              scan.mresultScanner.nextRows(scan.getMaxNumRows());

          // Convert to Arraylist of Keyvalue
          int num_rows = 0;
          // find out number of rows to return
          for (MapRResult mr : mresults) {
            if (mr.isEmpty()) {
              // break out after an empty row next rows should be empty too
              break;
            }
            ++num_rows;
          }

          if (num_rows == 0) { // no more rows, end of scan
            if (dummyRpc != null) {
              dummyRpc.callback(null);
            }
            continue;
          }

          final ArrayList<ArrayList<KeyValue>> rows =
                      new ArrayList<ArrayList<KeyValue>> (num_rows);
          for (int i = 0 ;i < num_rows; ++i) {
            int keyLen = mresults[i].getKeyLength();
            byte [] key = new byte[keyLen];
            System.arraycopy(mresults[i].bufBytes, 0, key, 0, mresults[i].getKeyLength());
            ArrayList<KeyValue> kv =
                MapRConverter.toAsyncHBaseResult(mresults[i], key, mTable);
            rows.add(kv);
          }

          // Callback
          dummyRpc.callback(rows);
        } catch (StackOverflowError e) {
          // Can happen if the callback chain run really deep, see Bug 23881.
          LOG.debug(String.format("ScanRpcRunnable::StackOverflowError: {}, ThreadId: {}",
              e.getMessage(), Thread.currentThread().getId()), e);
        } catch (Throwable e) { // do not let the thread die
          LOG.error(String.format("ScanRpcRunnable::{}: {}, ThreadId: {}",
              e.getClass().getSimpleName(), e.getMessage(), Thread.currentThread().getId()), e);
          if (dummyRpc != null) {
            dummyRpc.callback(e);
            Deferred.fromError(e instanceof Exception ? (Exception) e : new Exception(e));
          }
        }
      }
    }
  }

  static class CallBackRunnable implements Runnable {
    LinkedList<Object> requests;
    LinkedList<Object> responses;

    public CallBackRunnable(LinkedList<Object> requests, LinkedList<Object> responses) {
      this.requests = requests;
      this.responses = responses;
    }

    public void run() {
      for (Object request : requests) {
        Object result = responses.remove();
        try {
          if (request != null && request instanceof HBaseRpc) {
            HBaseRpc rpc = (HBaseRpc) request;
            rpc.callback(result);
          }
        } catch(Throwable e) {
          LOG.error("Exception in async CallBackRunnable.run(): " + e.getMessage(), e);
        }
      }
    }
  }
}
