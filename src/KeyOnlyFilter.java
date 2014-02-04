package org.hbase.async;

import org.hbase.async.generated.FilterPB;
import org.jboss.netty.buffer.ChannelBuffer;

public class KeyOnlyFilter extends ScanFilter {
  private static final byte[] NAME = Bytes.ISO88591("org.apache.hadoop"
      + ".hbase.filter.KeyOnlyFilter");

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  byte[] serialize() {
    return FilterPB.KeyOnlyFilter.newBuilder()
        .build()
        .toByteArray();
  }

  @Override
  void serializeOld(final ChannelBuffer buf) {
    buf.writeByte((byte) NAME.length);
    buf.writeBytes(NAME);
  }

  @Override
  int predictSerializedSize() {
    return 1 + NAME.length + 3;
  }
  
  @Override
  public String toString() {
    return "KeyOnlyFilter";
  }
}
