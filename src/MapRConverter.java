/* Copyright (c) 2012 & onwards. MapR Tech, Inc., All rights reserved */
package org.hbase.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.fs.MapRHTable;
import com.mapr.fs.jni.MapRConstants.RowConstants;
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
                                  String family, MapRThreadPool cbq) {
    int id = 0;
    int ncells = put.qualifiers().length;
    // get family id
    try {
      id = mtable.getFamilyId(family);
    } catch (IOException ioe) {
      throw new IllegalArgumentException("Invalid column family " +
                                         family, ioe);
    }
    if (ncells > 1) {
      // sort by qualifiers
      Integer []cells = new Integer[ncells];
      for (int i = 0 ; i< ncells; ++i) {
        cells[i] = i;
      }

      final byte [][]quals = put.qualifiers();
      final byte [][]vals = put.values();
      /* Pure java comparator from hbase */
      Comparator<Integer> cmp = new Comparator<Integer>(){
        public int compare(Integer i, Integer j) {
          return compareTo(quals[i], 0, quals[i].length,
              quals[j], 0, quals[j].length);
        }
      };

      Arrays.sort(cells, cmp);

      byte[][] sortedQuals = new byte[ncells][];
      byte[][] sortedVals = new byte[ncells][];

      for (int i = 0; i < ncells; ++i) {
        int off = cells[i].intValue();
        sortedQuals[i] = quals[off];
        sortedVals[i] = vals[off];
      }
      return new MapRPut(put.key(), id,
          sortedQuals, sortedVals, put.timestamp(),
          /* original request = */ put, /* callback on = */ cbq);
    } else {
      return new MapRPut(put.key(), id,
          put.qualifiers(), put.values(), put.timestamp(),
          /* original request = */ put, /* callback on = */ cbq);
    }
  }

  public static MapRGet toMapRGet(GetRequest get, MapRHTable mtable, String family) {
    try {
      MapRGet mrget = new MapRGet();
      // save key
      mrget.key = get.key();
      mrget.result = new MapRResult();
      // set constraints
      mrget.rowConstraint = toRowConstraint(mtable, family, get.qualifiers(),
					    get.maxVersions());
      return mrget;
    } catch (IOException ioe) {
      throw new IllegalArgumentException("Invalid column family " + family, ioe);
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
    ByteBuffer bbuf = mresult.getByteBuf();
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
                                             Bytes.toString(scan.getFamily()),
                                             scan.getQualifiers(),
                                             scan.getMinTimestamp(),
                                             scan.getMaxTimestamp(),
					     scan.getMaxVersions());
    return maprscan;
  }




  // Build rowconstraint for async scan
  public static MapRRowConstraint toRowConstraint(MapRHTable mtable,
                                                  String family,
                                                  byte[][] qualifiers,
                                                  long minTimeStamp,
                                                  long maxTimeStamp,
						  int maxVers)
       throws IOException {

    MapRRowConstraint rc = new MapRRowConstraint();

    //create columns and families array
    if (family != null) {
      int id = 0;
      try {
        // get family id
        id = mtable.getFamilyId(family);
      } catch (IOException ioe) {
        throw new IOException("Invalid column family " + family, ioe);
      }
      rc.numFamilies = 1;
      rc.families = new int[rc.numFamilies];
      rc.families[0] = id;
      rc.columnsPerFamily = new int[rc.numFamilies];
      rc.columnsPerFamily[0] = 1;
    } else {
      rc.numFamilies = 0;
      rc.families = new int[rc.numFamilies];
      rc.columnsPerFamily = new int[rc.numFamilies];
    }

    if (qualifiers != null) {
      rc.numColumns = qualifiers.length;
      rc.columns  = new byte[rc.numColumns][];
      if (family != null)
        rc.columnsPerFamily[0] = rc.numColumns;

      int i = 0;
      for (byte[] q: qualifiers) {
        rc.columns[i] = q;
        i++;
      }
    } else {
      rc.numColumns = 0;
      if (family != null)
        rc.columnsPerFamily[0] = 0;
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
                                          RowConstants.DEFAULT_MIN_STAMP,
                                          RowConstants.DEFAULT_MAX_STAMP,
					  RowConstants.DEFAULT_MAX_VERSIONS);
    mincr.deltas = new long[1];
    mincr.deltas[0] = delta;
    return mincr;
  }
}
