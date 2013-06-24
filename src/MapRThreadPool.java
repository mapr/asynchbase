/* Copyright (c) 2012 & onwards. MapR Tech, Inc., All rights reserved */
package org.hbase.async;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public AsyncHBaseRpc(HBaseRpc rpc, MapRHTable mTable, Deferred<?> deferred) {
      this(rpc, mTable);
      this.deferred = deferred;
    }

    public AsyncHBaseRpc(HBaseRpc rpc, MapRHTable mTable, Deferred<?> deferred, Scanner scanner) {
      this(rpc, mTable, deferred);
      this.scanner = scanner;
    }
  }

  // pool for all rpcs except scanner
  public static final int MIN_THREADS = 5;
  public static final int MAX_THREADS = 10;
  private ExecutorService pool;
  BlockingQueue<AsyncHBaseRpc> asyncrpcQueue;

  // Separate pool for scanner
  private ExecutorService scanpool;

  MapRThreadPool() {
    asyncrpcQueue = new LinkedBlockingQueue<AsyncHBaseRpc> ();

    pool = new ThreadPoolExecutor(
                  1 + MIN_THREADS, // core thread pool size, should be atleast 1 more than MIN_THREADS
                  MAX_THREADS, // maximum thread pool size
                  1, // time to wait before resizing pool
                  TimeUnit.HOURS,
                  new LinkedBlockingQueue<Runnable>(),
                  new ThreadPoolExecutor.CallerRunsPolicy());
    // Start minimum number of threads in pool.
    for (int i = 0; i < MIN_THREADS; i++)
       pool.execute(new RpcRunnable());

    // Separate threadpool for Scanner, with unlimited threads
    // This threadpool will run when scan is invoked. No need to execute it now
    scanpool = new ThreadPoolExecutor(
                  0, // core thread pool size
                  Integer.MAX_VALUE, // maximum thread pool size
                  1, // time to wait before resizing pool
                  TimeUnit.HOURS,
                  new LinkedBlockingQueue<Runnable>(),
                  new ThreadPoolExecutor.CallerRunsPolicy());

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

  // Used for scan()
  public ScanRpcRunnable newScanner(MapRHTable mtable, Scanner scan) {
    ScanRpcRunnable sc = new ScanRpcRunnable(mtable, scan);
    scanpool.execute(sc);
    return sc;
  }

  public void closeScanner(Deferred<?> d, MapRHTable mTable, Scanner scan) {
    AsyncHBaseRpc asrpc = new AsyncHBaseRpc(null, mTable, d, scan);
    asyncrpcQueue.add(asrpc);
  }

  // Used for callback/errback
  public void runCallbackChain(LinkedList<Object> requests, LinkedList<Object> responses) {
    try {
      pool.execute(new CallBackRunnable(requests, responses));
    } catch(Exception e) {
      // AH TODO handle rejection exception
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
            MapRGet mGet = MapRConverter.toMapRGet(grpc, mTable,
                                                   Bytes.toString(grpc.family()));
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

            byte[][] qualifiers = new byte [1][];
            qualifiers[0] = crpc.qualifier();
            byte[][] values = new byte [1][];
            MapRPut mput;
            boolean res = false;

            if (crpc.value() == null) {
              // Special case: treat as delete

              values[0] = new byte [0];
              mput =  new MapRPut(crpc.key(), id, qualifiers, values,
                                  KeyValue.TIMESTAMP_NOW,
                                  PutConstants.TYPE_DELETE_CELLS_ASYNC);
              res = mTable.checkAndDelete(crpc.key(), useCf, id,
                                          crpc.qualifier(), crpc.expectedValue(),
                                          mput);
            } else {
              values[0] = crpc.value();
              mput =  new MapRPut(crpc.key(), id, qualifiers, values,
                                  KeyValue.TIMESTAMP_NOW);
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
            int id = 0;
            // get family id

            DeleteRequest drpc = (DeleteRequest)rpc;
            if (drpc.family() != null) {
              String family = Bytes.toString(drpc.family());
              if (!family.isEmpty()) {
                try {
                  id = mTable.getFamilyId(family);
                } catch (IOException ioe) {
                  throw new IllegalArgumentException("Invalid column family " +
                                                     family, ioe);
                }
              }
            }

            byte[][] values = new byte [1][]; // Just a dummy
            values[0] = new byte [0];
            byte type = (drpc.family() == DeleteRequest.WHOLE_ROW) ?
                PutConstants.TYPE_DELETE_ROW_ASYNC : PutConstants.TYPE_DELETE_CELLS_ASYNC;
            MapRPut mput;
            if (type == PutConstants.TYPE_DELETE_CELLS_ASYNC) {
              mput =  new MapRPut(drpc.key(), id, drpc.qualifiers(), values,
                                  drpc.timestamp(), type);
            } else {
              mput =  new MapRPut(drpc.key(), id, values, values,
                                  drpc.timestamp(), type);
            }
            mTable.delete(mput);
            drpc.callback(null);
          } catch (Exception e) {
            LOG.error("Exception in async CompareAndSetRequest: " + e.getMessage());
            rpc.callback(e);
            Deferred.fromError(e);
            return;
          }
        }
      }
    }
  }

  // Used by scan
  static class MapRScanPlus {
    MapRScan mscan;
    HBaseRpc dummyRpc;

    MapRScanPlus(MapRScan mscan, HBaseRpc dummyRpc) {
      this.mscan = mscan;
      this.dummyRpc = dummyRpc;
    }
  }

  static class ScanRpcRunnable implements Runnable {
    MapRHTable mTable;
    Scanner scan;
    BlockingQueue<MapRScanPlus> scanRequestQueue;
    MapRScan mscan;

    ScanRpcRunnable(MapRHTable mt, Scanner sc) {
      this.mTable = mt;
      this.scan = sc;
      this.scanRequestQueue = new LinkedBlockingQueue<MapRScanPlus>();
      this.mscan = null;
    }

    public void addRequest(HBaseRpc dummyRpc) {
      try {
        // NOTE: Do this only once
        if (this.mscan == null) {
          this.mscan = MapRConverter.toMapRScan(scan, mTable);
          if (this.scan.getFilterMsg() != null) {
            mscan.setFilter(this.scan.getFilterMsg().toByteArray());
          }
        }
        this.mscan.startRow = scan.getCurrentKey();
        this.mscan.stopRow = scan.getStopKey();
        MapRScanPlus mscanPlus = new MapRScanPlus(mscan, dummyRpc);
        if (scanRequestQueue.remainingCapacity() != 0) {
          scanRequestQueue.add(mscanPlus);
        } else {
          throw new UnknownScannerException("Failed to add scan request to queue", dummyRpc);
        }
      } catch (Exception e) {
        LOG.error("Exception in addRequest: " + e.getMessage());
        dummyRpc.callback(e);
        Deferred.fromError(e);
      }
    }

    public void run() {
      while (true) {
        HBaseRpc dummyRpc = null;
        try {
          // handle connection
          MapRScanPlus mscanPlus = scanRequestQueue.take();
          MapRScan mscan = mscanPlus.mscan;
          dummyRpc = mscanPlus.dummyRpc;

          // NOTE: Do this only once
          if (scan.mresultScanner == null) {
            scan.mresultScanner = new MapRResultScanner(mscan, mTable);
          }

          // Do the scan
          MapRResult[] mresults = scan.mresultScanner.nextRows(scan.getMaxNumRows());

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

          if (num_rows == 0) {
            scan.mresultScanner.releaseTempMemory();
            dummyRpc.callback(null);
            return;
          }

          final ArrayList<ArrayList<KeyValue>> rows =
                      new ArrayList<ArrayList<KeyValue>> (num_rows);
          for (int i = 0 ;i < num_rows; ++i) {
            int keyLen = mresults[i].getKeyLength();
            byte [] key = new byte[keyLen];
            mresults[i].getByteBuf().position(0);
            mresults[i].getByteBuf().get(key, 0, keyLen);
            ArrayList<KeyValue> kv =
              MapRConverter.toAsyncHBaseResult(mresults[i], key, mTable);
            rows.add(kv);
          }

          scan.mresultScanner.releaseTempMemory();

          // Callback
          dummyRpc.callback(rows);
        } catch (InterruptedException e) {
          break;
        } catch (Exception e) {
          LOG.error("Exception in scanner thread: " + e.getMessage());
          if (dummyRpc != null) {
            dummyRpc.callback(e);
          }
          Deferred.fromError(e);
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
        } catch (Exception e) {
          // ignore
        }
      }
    }
  }
}
