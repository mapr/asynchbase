/*
 * Copyright (C) 2014  The Async HBase Authors.  All rights reserved.
 * This file is part of Async HBase.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.hbase.async;

import org.hbase.async.generated.FilterPB;

import com.google.protobuf.ByteString;
import com.mapr.fs.proto.Dbfilters.FamilyFilterProto;

/**
 * Filter columns based on the column family. Takes an operator (equal, greater,
 * not equal, etc). and a filter comparator.
 * @since 1.6
 */
public final class FamilyFilter extends CompareFilter {

  private static final byte[] NAME =
      Bytes.UTF8("org.apache.hadoop.hbase.filter.FamilyFilter");

  public FamilyFilter(final CompareOp family_compare_op,
                      final FilterComparator family_comparator) {
    super(family_compare_op, family_comparator);
  }

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  byte[] serialize() {
    return FilterPB
        .FamilyFilter
        .newBuilder()
        .setCompareFilter(toProtobuf())
        .build()
        .toByteArray();
  }

  // MapR addition
  public static final int kFamilyFilter                    = 0x80e32faa;

  @Override
  protected ByteString getState() {
    return FamilyFilterProto.newBuilder()
        .setFilterComparator(toFilterComparatorProto())
        .build().toByteString();
  }

  @Override
  protected String getId() {
    return getFilterId(kFamilyFilter);
  }

}
