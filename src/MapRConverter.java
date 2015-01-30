/* Copyright (c) 2012 & onwards. MapR Tech, Inc., All rights reserved */
package org.hbase.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.lang.Integer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.fs.MapRHTable;
import com.mapr.fs.jni.MapRCallBackQueue;
import com.mapr.fs.jni.MapRConstants.RowConstants;
import com.mapr.fs.jni.MapRConstants.PutConstants;
import com.mapr.fs.jni.MapRGet;
import com.mapr.fs.jni.MapRIncrement;
import com.mapr.fs.jni.MapRPut;
import com.mapr.fs.jni.MapRResult;
import com.mapr.fs.jni.MapRRowConstraint;
import com.mapr.fs.jni.MapRScan;

public class MapRConverter {

  private static final Logger LOG = LoggerFactory.getLogger(MapRConverter.class);
  private static final byte[] ZERO_BYTE_ARRAY = new byte[0];

  // copied from hbase
  public static int compareTo(byte[] buffer1, int offset1, int length1,
                              byte[] buffer2, int offset2, int length2) {
    // Short circuit equal case
    if (buffer1 == buffer2 &&
        offset1 == offset2 &&
        length1 == length2) {
      return 0;
    }
    // Bring WritableComparator code local
    int end1 = offset1 + length1;
    int end2 = offset2 + length2;
    for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
      int a = (buffer1[i] & 0xff);
      int b = (buffer2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return length1 - length2;
  }

  public static MapRPut toMapRPut(PutRequest put, MapRHTable mtable,
                                  MapRThreadPool cbq) {
    byte[][] families     = put.getFamilies();
    byte[][][] qualifiers = put.getQualifiers();
    byte[][][] values     = put.getValues();

    int[] familyIds = new int[families.length];
    int nCells = 0;
    for (int i = 0; i < families.length; ++i) {
      String family = Bytes.toString(families[i]);

      try {
        familyIds[i] = mtable.getFamilyId(family);
      } catch (IOException ioe) {
        throw new IllegalArgumentException("Invalid column family " +
                                           family, ioe);
      }

      nCells += qualifiers[i].length;
    }

    byte[][][] sortedQuals = new byte[nCells][][];
    byte[][][] sortedVals  = new byte[nCells][][];

    if (nCells > 1) {
      for (int i = 0; i < families.length; ++i) {
        // sort by qualifiers
        int cellsPerFamily = qualifiers[i].length;
        Integer []cells = new Integer[cellsPerFamily];
        for (int j = 0 ; j < cellsPerFamily; ++j) {
          cells[j] = j;
        }
        sortedQuals[i] = new byte[cellsPerFamily][];
        sortedVals[i]  = new byte[cellsPerFamily][];

        final byte [][]quals = qualifiers[i];
        final byte [][]vals  = values[i];

        /* Pure java comparator from hbase */
        Comparator<Integer> cmp = new Comparator<Integer>(){
          public int compare(Integer i, Integer j) {
            return compareTo(quals[i], 0, quals[i].length,
                quals[j], 0, quals[j].length);
          }
        };

        Arrays.sort(cells, cmp);

        for (int j = 0; j < cellsPerFamily; ++j) {
          int off = cells[j].intValue();
          sortedQuals[i][j] = quals[off];
          sortedVals[i][j]  = vals[off];
        }
      }

      return new MapRPut(put.key(), familyIds,
          sortedQuals, sortedVals, put.timestamp(), put.getTimestamps(),
          /* original request = */ (Object)put, /* callback on = */ (MapRCallBackQueue)cbq);
    } else {
      return new MapRPut(put.key(), familyIds,
          put.getQualifiers(), put.getValues(), put.timestamp(), put.getTimestamps(),
          /* original request = */ (Object)put, /* callback on = */ (MapRCallBackQueue)cbq);
    }
  }

  public static MapRPut toMapRPut(DeleteRequest drpc, MapRHTable mTable) {
    MapRPut mput;
    if (drpc.getFamilies() != DeleteRequest.WHOLE_ROW) {
      byte[][] families = drpc.getFamilies();
      int[] familyIds = new int[families.length];
      for (int i = 0; i < families.length; i ++) {
        String family = Bytes.toString(families[i]);
        try {
          familyIds[i] = mTable.getFamilyId(family);
        } catch (IOException ioe) {
          throw new IllegalArgumentException("Invalid column family " +
                                             family, ioe);
        }
      }

      mput =  new MapRPut(drpc.key(), familyIds, drpc.getQualifiers(), 
                          /*values*/null, drpc.timestamp(), /*timestamps*/null,
                          PutConstants.TYPE_DELETE_CELLS_ASYNC);

    } else {
      mput =  new MapRPut(drpc.key(), /*familyIds*/null, /*qualifiers*/null, 
                          /*values*/null, drpc.timestamp(), /*timestamps*/null,
                          PutConstants.TYPE_DELETE_ROW_ASYNC);
    }

    return mput;
  }


  public static MapRGet toMapRGet(GetRequest get, MapRHTable mtable) {
    try {
      MapRGet mrget = new MapRGet();
      // save key
      mrget.key = get.key();
      mrget.result = new MapRResult();
      // set constraints
      mrget.rowConstraint = toRowConstraint(mtable, get.getFamilies(), get.getQualifiers(),
                                            RowConstants.DEFAULT_MIN_STAMP,
                                            RowConstants.DEFAULT_MAX_STAMP,
                                            get.maxVersions());
      return mrget;
    } catch (IOException ioe) {
      throw new IllegalArgumentException("Invalid column families " + get.getFamilies(), ioe);
    }
  }

  // Build rowconstraint for async get
  public static MapRRowConstraint toRowConstraint( MapRHTable htable,
                                                   String family,
                                                   byte[][] qualifier,
                                                   int maxVers)
  throws IOException {

    MapRRowConstraint rc = new MapRRowConstraint();

    if (family != null) {
      int id = 0;
      try {
        // get family id
        id = htable.getFamilyId(family);
      } catch (IOException ioe) {
        throw new IOException("Invalid column family " + family, ioe);
      }
      rc.numFamilies = 1;
      rc.families = new int[rc.numFamilies];
      rc.families[0] = id;
      rc.columnsPerFamily = new int[rc.numFamilies];
    } else {
      rc.numFamilies = 0;
      rc.families = new int[rc.numFamilies];
      rc.columnsPerFamily = new int[rc.numFamilies];
    }

    if (qualifier != null) {
      rc.columns  = new byte[qualifier.length][];
      rc.numColumns = qualifier.length;
      if (family != null)
        rc.columnsPerFamily[0] = qualifier.length;

      int i = 0;
      for (byte[] q: qualifier) {
        rc.columns[i] = q;
        i++;
      }
    } else {
      rc.numColumns = 0;
      if (family != null)
        rc.columnsPerFamily[0] = 0;
    }

    rc.maxVersions = maxVers;
    rc.minStamp = RowConstants.DEFAULT_MIN_STAMP;
    rc.maxStamp = RowConstants.DEFAULT_MAX_STAMP;
    return rc;
  }

  // From result -> async get()
  public static ArrayList<KeyValue> toAsyncHBaseResult(MapRResult mresult,
                                                       byte[] key,
                                                       MapRHTable htable) {
    if (mresult == null ||
        mresult.getColumnOffsets() == null ||
        mresult.getBufSize() == 0) {
      return new ArrayList<KeyValue> ();
    }

    String cfname;
    ArrayList<KeyValue> cells = new ArrayList<KeyValue> (mresult.getColumnOffsets().length);
    ByteBuffer bbuf = ByteBuffer.wrap(mresult.bufBytes);
    int columnPos = 0;
    int cellPos = 0;

    for (int f = 0; f < mresult.getCfids().length; ++f) {
      // get family name
      try {
        cfname = htable.getFamilyName(mresult.getCfids()[f]);
      } catch (IOException ioe) {
        // AH TODO LOG error
        if (key != null) {
          LOG.error("Stale column family id: " + mresult.getCfids()[f] +
                    ". Ignoring get request with key = " +
                     Bytes.toString(key) + "]");
        } else {
          LOG.error("Stale column family id: " + mresult.getCfids()[f] +
                    ", during scan");
        }
        continue;
      }

      byte[] fname = cfname.getBytes();

      for (int i = 0; i < mresult.getCellsPerFamily()[f]; ++i) {
	int qualLen = mresult.getColumnLengths()[columnPos];
	byte[] qual = ZERO_BYTE_ARRAY;;
	if (qualLen != 0) {
	  qual = new byte[qualLen];
	  bbuf.position(mresult.getColumnOffsets()[columnPos]);
	  bbuf.get(qual, 0, qualLen);
        }

	for (int v = 0; v < mresult.versions()[columnPos]; ++v) {
	   int valueLen = mresult.getValueLengths()[cellPos];
	   byte[] val = new byte[valueLen];
	   bbuf.position(mresult.getValueOffsets()[cellPos]);
	   bbuf.get(val, 0, valueLen);

           KeyValue kv = new KeyValue(key, fname, qual,
				      mresult.getTimeStamps()[cellPos], val);
           cells.add(kv);
           cellPos++;
         }
         ++columnPos;
      }
    }

    /*
     *  Need to do this since within a MapR result, unlike HBase, KeyValue is
     *  sorted on column family IDs (a number) instead of column family name.
     *  This sort is guaranteed to be stable. Equal elements will not be
     *  reordered as a result of the sort.
     */
     Collections.sort(cells, new Comparator<KeyValue>() {
       @Override
       public int compare(KeyValue kv1, KeyValue kv2) {
         return Bytes.memcmp(kv1.family(), kv2.family());
       }
     });
     return cells;
  }

  // From async scan -> MapR scan
  public static MapRScan toMapRScan(Scanner scan, MapRHTable mtable)
        throws IOException {
    MapRScan maprscan = new MapRScan();
    maprscan.startRow = scan.getCurrentKey();
    maprscan.stopRow = scan.getStopKey();

    // set constraints
    // TODO: MapR, Need to support Scanner regex and max_num_kvs
    maprscan.rowConstraint = toRowConstraint(mtable,
                                             scan.getFamilies(),
                                             scan.getQualifiers(),
                                             scan.getMinTimestamp(),
                                             scan.getMaxTimestamp(),
					                                   scan.getMaxVersions());
    return maprscan;
  }

  // Build rowconstraint for async scan
  public static MapRRowConstraint toRowConstraint(MapRHTable mtable,
                                                  byte[][]   families,
                                                  byte[][][] qualifiers,
                                                  long       minTimeStamp,
                                                  long       maxTimeStamp,
                                                  int maxVers)
       throws IOException {

    MapRRowConstraint rc = new MapRRowConstraint();
    //create columns and families array
    if (families != null) {
      rc.numFamilies = families.length;
      rc.families = new int[rc.numFamilies];
      rc.columnsPerFamily = new int[rc.numFamilies];
      rc.numColumns = 0;
      for (int i = 0; i < families.length; ++i) {
        try {
          // get family id
          rc.families[i] = mtable.getFamilyId(Bytes.toString(families[i]));
        } catch (IOException ioe) {
          throw new IOException("Invalid columns family " + families[i], ioe);
        }

        if ((qualifiers != null) && (qualifiers[i] != null)) {
          rc.columnsPerFamily[i] = qualifiers[i].length;
        } else {
          rc.columnsPerFamily[i] = 0;
        }

        rc.numColumns += rc.columnsPerFamily[i];
      }

      if (qualifiers != null) {
        rc.columns = new byte[rc.numColumns][];
        int k = 0;
        for (int i = 0; i < qualifiers.length; i ++) {
          for (int j = 0; (qualifiers[i] != null) && (j < qualifiers[i].length); j ++) {
            rc.columns[k] = qualifiers[i][j];
            k ++;
          }
        }
      } else {
        rc.columns = null;
      }
    } else {
      rc.numFamilies = 0;
      rc.families = null;
      rc.columnsPerFamily = null;
      rc.numColumns = 0;
    }

    rc.maxVersions = maxVers;
    rc.minStamp = minTimeStamp;
    rc.maxStamp = maxTimeStamp;
    return rc;
  }

  // From async incr -> MapR incr
  public static MapRIncrement toMapRIncrement(byte []key, byte []family,
                                              byte []qualifier, long delta,
                                              MapRHTable mtable)
     throws IOException {
    MapRIncrement mincr = new MapRIncrement();
    // save key
    mincr.key = key;
    // set constraints
    mincr.rowConstraint = toRowConstraint(mtable, Bytes.toString(family),
                                          new byte[][] {qualifier},
                                          RowConstants.DEFAULT_MAX_VERSIONS);
    mincr.deltas = new long[1];
    mincr.deltas[0] = delta;
    return mincr;
  }
}
