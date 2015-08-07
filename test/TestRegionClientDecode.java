/*
 * Copyright (C) 2015 The Async HBase Authors.  All rights reserved.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ReadOnlyByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.security.sasl.SaslClient;
import javax.xml.bind.DatatypeConverter;

import org.hbase.async.auth.SimpleClientAuthProvider;
import org.hbase.async.generated.CellPB.Cell;
import org.hbase.async.generated.ClientPB.GetResponse;
import org.hbase.async.generated.ClientPB.Result;
import org.hbase.async.generated.RPCPB;
import org.hbase.async.generated.RPCPB.CellBlockMeta;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;

import com.google.protobuf.CodedOutputStream;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

@PrepareForTest({ GetRequest.class, ChannelHandlerContext.class })
public class TestRegionClientDecode extends BaseTestRegionClient {
  private static final byte[] ROW = { 0, 0, 1 };
  private static final byte[] FAMILY = { 'n', 'o', 'b' };
  private static final byte[] TABLE = { 'd', 'w' };
  private static final byte[] QUALIFIER = { 'v', 'i', 'm', 'e', 's' };
  private static final byte[] VALUE = { 42 };
  private static final long TIMESTAMP = 1356998400000L;
  
  // NOTE: the TYPE of ByteBuf is important! ReplayingDecoderBuffer isn't
  // backed by an array and we have methods that attemp to see if they can
  // perform zero copy operations.
  
  @Test
  public void goodGetRequest() throws Exception {
    final int id = 42;
    final ByteBuf buffer = buildGoodResponse(false, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    rpcs_inflight.put(id, get);
    
    region_client.decode(ctx, buffer, null);
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.joinUninterruptibly();
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    verify(secure_rpc_helper, never()).handleResponse(buffer, chan);
  }
  
  @Test
  public void goodGetRequestArrayBacked() throws Exception {
    final int id = 42;
    final ByteBuf buffer = buildGoodResponse(true, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    rpcs_inflight.put(id, get);
    
    region_client.decode(ctx, buffer, null);
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.joinUninterruptibly();
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    verify(secure_rpc_helper, never()).handleResponse(buffer, chan);
  }

  @Test
  public void goodGetRequestWithSecurity() throws Exception {
    injectSecurity();
    final int id = 42;
    final ByteBuf buffer = buildGoodResponse(false, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    rpcs_inflight.put(id, get);
    
    region_client.decode(ctx, buffer, null);
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.joinUninterruptibly();
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    verify(secure_rpc_helper, times(1)).handleResponse(buffer, chan);
  }
  
  @Test
  public void goodGetRequestWithSecurityArrayBacked() throws Exception {
    injectSecurity();
    final int id = 42;
    final ByteBuf buffer = buildGoodResponse(true, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    rpcs_inflight.put(id, get);
    
    region_client.decode(ctx, buffer, null);
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.joinUninterruptibly();
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    verify(secure_rpc_helper, times(1)).handleResponse(buffer, chan);
  }
  
  @Test
  public void goodGetRequestWithSecurityConsumesAll() throws Exception {
    injectSecurity();
    final int id = 42;
    final ByteBuf buffer = buildGoodResponse(false, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    rpcs_inflight.put(id, get);
    
    when(secure_rpc_helper.handleResponse(buffer, chan)).thenReturn(null);
    
    region_client.decode(ctx, buffer, null);
    
    Exception e = null;
    try {
      deferred.join(500);
      fail("Expected to fail the join");
    } catch (TimeoutException te) {
      e = te;
    }
    assertNotNull(e);
  }
  
  @Test
  public void goodGetRequestWithSecurityConsumesAllArrayBacked() throws Exception {
    injectSecurity();
    final int id = 42;
    final ByteBuf buffer = buildGoodResponse(true, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    rpcs_inflight.put(id, get);
    
    when(secure_rpc_helper.handleResponse(buffer, chan)).thenReturn(null);
    
    region_client.decode(ctx, buffer, null);
    
    Exception e = null;
    try {
      deferred.join(500);
      fail("Expected to fail the join");
    } catch (TimeoutException te) {
      e = te;
    }
    assertNotNull(e);
  }
  
  @Test
  public void pbufDeserializeFailure() throws Exception {
    // in this case we have a good length and header but the actual pbuf result
    // is missing. We pull the rpc from the inflight map and call it back with
    // the exception.
    final int id = 42;
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    rpcs_inflight.put(id, get);
    
    ByteBuf buffer = new ReadOnlyByteBuf(
        Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 3, 2, 8, 42 }));
    RuntimeException e = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected a NonRecoverableException");
    } catch (NonRecoverableException ex) {
      e = ex;
    }
    assertTrue(e instanceof NonRecoverableException);
    
    e = null;
    try {
      deferred.join();
      fail("Expected the join to throw a NonRecoverableException");
    } catch (NonRecoverableException ex) {
      e = ex;
    }
    assertTrue(e instanceof NonRecoverableException);
    assertEquals(0, rpcs_inflight.size());  
  }
  
  @Test
  public void pbufDeserializeFailureArrayBacked() throws Exception {
    // in this case we have a good length and header but the actual pbuf result
    // is missing. We pull the rpc from the inflight map and call it back with
    // the exception.
    final int id = 42;
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    rpcs_inflight.put(id, get);
    
    ByteBuf buffer = 
        Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 3, 2, 8, 42 });
    RuntimeException e = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected a NonRecoverableException");
    } catch (NonRecoverableException ex) {
      e = ex;
    }
    assertTrue(e instanceof NonRecoverableException);
    
    e = null;
    try {
      deferred.join();
      fail("Expected the join to throw a NonRecoverableException");
    } catch (NonRecoverableException ex) {
      e = ex;
    }
    assertTrue(e instanceof NonRecoverableException);
    assertEquals(0, rpcs_inflight.size());  
  }
  
  @Test
  public void pbufDeserializeFailureWHBaseException() throws Exception {
    // not entirely sure how this would happen, maybe if there was an exception
    // encoded in the cell meta header or something?
    final int id = 42;
    final GetRequest get = mock(GetRequest.class);
    final Deferred<Object> deferred = get.getDeferred();
    when(get.deserialize(any(ByteBuf.class), anyInt()))
      .thenThrow(new TestingHBaseException("Boo!"));
    rpcs_inflight.put(id, get);
    
    ByteBuf buffer = new ReadOnlyByteBuf(
        Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 1, 2, 8, 42 }));
    RuntimeException e = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected a HBaseException");
    } catch (HBaseException ex) {
      e = ex;
    }
    assertTrue(e instanceof HBaseException);
    
    e = null;
    try {
      deferred.join();
      fail("Expected the join to throw a HBaseException");
    } catch (HBaseException ex) {
      e = ex;
    }
    assertTrue(e instanceof HBaseException);
    assertEquals(0, rpcs_inflight.size());
  }
  
  @Test
  public void nsre() throws Exception {
    final int id = 42;
    final GetRequest get = PowerMockito.mock(GetRequest.class);
    final NotServingRegionException nsre = 
        new NotServingRegionException("Boo!", get);
    when(get.deserialize(any(ByteBuf.class), anyInt()))
      .thenReturn(nsre);
    when(get.getRegion()).thenReturn(region);
    rpcs_inflight.put(id, get);
    
    ByteBuf buffer = new ReadOnlyByteBuf(
        Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 1, 2, 8, 42 }));
    region_client.decode(ctx, buffer, null);

    assertEquals(0, rpcs_inflight.size());
    verify(hbase_client, times(1))
      .handleNSRE(get, region.name(), nsre);
    verify(get, never()).callback(anyObject());
    verify(get, never()).getDeferred();
  }

  @Test
  public void replayed() throws Exception {
    resetMockClient();
    
    final int id = 42;
    final ByteBuf buffer = buildGoodResponse(true, id);
    final byte[] array = new byte[buffer.writerIndex()];
    buffer.readBytes(array);
    
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    rpcs_inflight.put(id, get);
    
    final byte[][] chunks = new byte[3][];
    chunks[0] = Arrays.copyOf(array, 3);
    chunks[1] = Arrays.copyOfRange(array, 3, 10);
    chunks[2] = Arrays.copyOfRange(array, 10, array.length);

    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[0]));
    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[1]));
    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[2]));
    
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.join(100);
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    verify(secure_rpc_helper, never()).handleResponse(buffer, chan);
  }
 
  @Test
  public void noReplayNeeded() throws Exception {
    resetMockClient();
    
    final int id = 42;
    final ByteBuf buffer = buildGoodResponse(true, id);
    final byte[] array = new byte[buffer.writerIndex()];
    buffer.readBytes(array);
    
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    rpcs_inflight.put(id, get);

    region_client.channelRead(ctx, Unpooled.wrappedBuffer(array));

    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.join(100);
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    verify(secure_rpc_helper, never()).handleResponse(buffer, chan);
  }
  
  @Test
  public void replayedMissingMiddle() throws Exception {
    resetMockClient();
    
    final int id = 42;
    final ByteBuf buffer = buildGoodResponse(true, id);
    final byte[] array = new byte[buffer.writerIndex()];
    buffer.readBytes(array);
    
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    rpcs_inflight.put(id, get);
    
    final byte[][] chunks = new byte[2][];
    chunks[0] = Arrays.copyOf(array, 3);
    chunks[1] = Arrays.copyOfRange(array, 10, array.length);

    RuntimeException e = null;
    try {
      region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[0]));
    } catch (RuntimeException ex) {
      e = ex;
    }
    assertNull(e);

    try {
      region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[1]));
    } catch (RuntimeException ex) {
      e = ex;
    }
    System.out.println(e);
    // TODO - used to be org.hbase.async.InvalidResponseException
    assertTrue(e instanceof DecoderException);

    try {
      deferred.join(100);
    }  catch (RuntimeException ex) {
      e = ex;
    }
    assertTrue(e instanceof TimeoutException);
  }

  @Test
  public void replayedSecure() throws Exception {
    resetMockClient();
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "simple");
    config.overrideConfig(SimpleClientAuthProvider.USERNAME_KEY, 
        "Cohen");
    
    final SecureRpcHelper96 secure_helper = 
        PowerMockito.spy(new SecureRpcHelper96(hbase_client, 
            region_client, new InetSocketAddress("127.0.0.1", 50512)));
    final SaslClient sasl_client = mock(SaslClient.class);
    Whitebox.setInternalState(secure_helper, "sasl_client", sasl_client);
    when(sasl_client.isComplete()).thenReturn(false);

    Whitebox.setInternalState(region_client, "secure_rpc_helper", secure_helper);

    PowerMockito.when(secure_helper.processChallenge(any(byte[].class)))
      .thenReturn(new byte[] { 24 });
    
    final int id = 42;
    final byte[] array = { 0, 0, 0, 0, 0, 0, 0, 4, 42, 24, 42, 24 };
    
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    rpcs_inflight.put(id, get);
    
    final byte[][] chunks = new byte[2][];
    chunks[0] = Arrays.copyOf(array, 3);
    chunks[1] = Arrays.copyOfRange(array, 10, array.length);

    region_client.channelRead(ctx, Unpooled.wrappedBuffer(Arrays.copyOf(array, 3)));
    region_client.channelRead(ctx, Unpooled.wrappedBuffer(Arrays.copyOfRange(array, 3, 6)));
    region_client.channelRead(ctx, Unpooled.wrappedBuffer(Arrays.copyOfRange(array, 6, 11)));
    region_client.channelRead(ctx, Unpooled.wrappedBuffer(Arrays.copyOfRange(array, 11, array.length)));
    
    RuntimeException e = null;
    try {
      deferred.join(100);
    } catch (RuntimeException ex) {
      e = ex;
    }
    assertTrue(e instanceof TimeoutException);
  }
  
  @Test
  public void replayMultiRPCInBuffer() throws Exception {
    resetMockClient();
    
    List<Deferred<Object>> deferreds = new ArrayList<Deferred<Object>>(3);
    
    final GetRequest get1 = new GetRequest(TABLE, ROW);
    deferreds.add(get1.getDeferred());
    rpcs_inflight.put(1, get1);
    
    final GetRequest get2 = new GetRequest(TABLE, ROW);
    deferreds.add(get2.getDeferred());
    rpcs_inflight.put(2, get2);
    
    final GetRequest get3 = new GetRequest(TABLE, ROW);
    deferreds.add(get3.getDeferred());
    rpcs_inflight.put(3, get3);
    
    byte[] pbuf1 = buildGoodResponse(true, 1).array();
    byte[] pbuf2 = buildGoodResponse(true, 2).array();
    byte[] pbuf3 = buildGoodResponse(true, 3).array();
    
    // chunk these guys up
    byte[][] chunks = new byte[3][];
    chunks[0] = Arrays.copyOf(pbuf1, pbuf1.length / 2);
    
    int len = pbuf1.length - (pbuf1.length / 2);
    byte[] buf = new byte[len + pbuf2.length / 2];
    System.arraycopy(pbuf1, pbuf1.length / 2, buf, 0, len);
    System.arraycopy(pbuf2, 0, buf, len, pbuf2.length / 2);
    chunks[1] = buf;
    
    len = pbuf2.length - (pbuf2.length / 2);
    chunks[2] = Arrays.copyOfRange(pbuf2, pbuf2.length / 2, pbuf2.length);

    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[0]));
    // we gave netty just a fragment of the first RPC so it will throw an error

    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[1]));
    // now netty has the full RPC1 AND a chunk of RPC2. We'll parse all of RPC1
    // and replay to get the next chunk of 2
    
    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[2]));
    // at this point we have read all of the data from the buffer so Netty will
    // discard it
    
    region_client.channelRead(ctx, Unpooled.wrappedBuffer(pbuf3));
    
    for (final Deferred<Object> deferred : deferreds) {
      @SuppressWarnings("unchecked")
      final List<KeyValue> kvs = (List<KeyValue>)deferred.join(100);
      assertEquals(1, kvs.size());
      assertArrayEquals(ROW, kvs.get(0).key());
      assertArrayEquals(FAMILY, kvs.get(0).family());
      assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
      assertArrayEquals(VALUE, kvs.get(0).value());
      assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void replayCorruptSecondRPC() throws Exception {
    resetMockClient();
    
    List<Deferred<Object>> deferreds = new ArrayList<Deferred<Object>>(3);
    
    final GetRequest get1 = new GetRequest(TABLE, ROW);
    deferreds.add(get1.getDeferred());
    rpcs_inflight.put(1, get1);
    
    final GetRequest get2 = new GetRequest(TABLE, ROW);
    deferreds.add(get2.getDeferred());
    rpcs_inflight.put(2, get2);
    
    final GetRequest get3 = new GetRequest(TABLE, ROW);
    deferreds.add(get3.getDeferred());
    rpcs_inflight.put(3, get3);
    
    byte[] pbuf1 = buildGoodResponse(true, 1).array();
    byte[] pbuf2 = buildGoodResponse(true, 2).array();
    // corrupt it
    pbuf2 = Arrays.copyOf(pbuf2, pbuf2.length - 4);
    byte[] pbuf3 = buildGoodResponse(true, 3).array();
    
    // chunk these guys up
    byte[][] chunks = new byte[3][];
    chunks[0] = Arrays.copyOf(pbuf1, pbuf1.length / 2);
    
    int len = pbuf1.length - (pbuf1.length / 2);
    byte[] buf = new byte[len + pbuf2.length / 2];
    System.arraycopy(pbuf1, pbuf1.length / 2, buf, 0, len);
    System.arraycopy(pbuf2, 0, buf, len, pbuf2.length / 2);
    chunks[1] = buf;
    
    len = pbuf2.length - (pbuf2.length / 2);
    chunks[2] = Arrays.copyOfRange(pbuf2, pbuf2.length / 2, pbuf2.length);

    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[0]));
    // we gave netty just a fragment of the first RPC so it will throw an error

    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[1]));
    // now netty has the full RPC1 AND a chunk of RPC2. We'll parse all of RPC1
    // and replay the rest 

    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[2]));
    // because our corrupted RPC is missing a little bit of data at the end 
    // we will proceed to replay
    
    RuntimeException ex = null;
    try {
      region_client.channelRead(ctx, Unpooled.wrappedBuffer(pbuf3));
      fail("Expected an InvalidResponseException");
    } catch (RuntimeException e) {
      ex = e;
    }
 // TODO - used to be org.hbase.async.InvalidResponseException
    assertTrue(ex instanceof DecoderException);

    // Make sure the first RPC was called back
    List<KeyValue> kvs = (List<KeyValue>)deferreds.get(0).join(100);
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
   
    // The second will toss the invalid RPC exception, causing the region client
    // to close.
    ex = null;
    try {
      kvs = (List<KeyValue>)deferreds.get(1).join(100);
      fail("Expected a TimeoutException");
    } catch (RuntimeException e) {
      ex = e;
    }
    assertTrue(ex instanceof InvalidResponseException);
    
    // and the third will never have been called
    ex = null;
    try {
      kvs = (List<KeyValue>)deferreds.get(2).join(100);
      fail("Expected a TimeoutException");
    } catch (RuntimeException e) {
      ex = e;
    }
    assertTrue(ex instanceof TimeoutException);
  }

  @Test
  public void replayMoreChunks() throws Exception {
    resetMockClient();
    
    List<Deferred<Object>> deferreds = new ArrayList<Deferred<Object>>(3);
    
    final GetRequest get1 = new GetRequest(TABLE, ROW);
    deferreds.add(get1.getDeferred());
    rpcs_inflight.put(1, get1);
    
    final GetRequest get2 = new GetRequest(TABLE, ROW);
    deferreds.add(get2.getDeferred());
    rpcs_inflight.put(2, get2);
    
    final GetRequest get3 = new GetRequest(TABLE, ROW);
    deferreds.add(get3.getDeferred());
    rpcs_inflight.put(3, get3);
    
    byte[] pbuf1 = buildGoodResponse(true, 1).array();
    byte[] pbuf2 = buildGoodResponse(true, 2).array();
    byte[] pbuf3 = buildGoodResponse(true, 3).array();
    
    // chunk these guys up
    byte[][] chunks = new byte[6][];
    chunks[0] = Arrays.copyOf(pbuf1, pbuf1.length / 2);
    
    int len = pbuf1.length - (pbuf1.length / 2);
    byte[] buf = new byte[len + pbuf2.length / 2];
    System.arraycopy(pbuf1, pbuf1.length / 2, buf, 0, len);
    System.arraycopy(pbuf2, 0, buf, len, pbuf2.length / 2);
    chunks[1] = buf;

    len = pbuf2.length / 2;
    chunks[2] = Arrays.copyOfRange(pbuf2, len, len + 4);
    chunks[3] = Arrays.copyOfRange(pbuf2, len + 4, pbuf2.length);
    
    len = pbuf3.length / 2;
    chunks[4] = Arrays.copyOfRange(pbuf3, 0, len);
    chunks[5] = Arrays.copyOfRange(pbuf3, len, pbuf3.length);

    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[0]));
    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[1]));
    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[2]));
    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[3]));
    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[4]));
    region_client.channelRead(ctx, Unpooled.wrappedBuffer(chunks[5]));
    
    for (final Deferred<Object> deferred : deferreds) {
      @SuppressWarnings("unchecked")
      final List<KeyValue> kvs = (List<KeyValue>)deferred.join(100);
      assertEquals(1, kvs.size());
      assertArrayEquals(ROW, kvs.get(0).key());
      assertArrayEquals(FAMILY, kvs.get(0).family());
      assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
      assertArrayEquals(VALUE, kvs.get(0).value());
      assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    }
  }
  
  @Test
  public void nsreArrayBacked() throws Exception {
    final int id = 42;
    final GetRequest get = PowerMockito.mock(GetRequest.class);
    final NotServingRegionException nsre = 
        new NotServingRegionException("Boo!", get);
    when(get.deserialize(any(ByteBuf.class), anyInt()))
      .thenReturn(nsre);
    when(get.getRegion()).thenReturn(region);
    rpcs_inflight.put(id, get);
    
    ByteBuf buffer = 
        Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 1, 2, 8, 42 });
    region_client.decode(ctx, buffer, null);

    assertEquals(0, rpcs_inflight.size());
    verify(hbase_client, times(1))
      .handleNSRE(get, region.name(), nsre);
    verify(get, never()).callback(anyObject());
    verify(get, never()).getDeferred();
  }
  
  @Test
  public void regionMovedException() throws Exception {
    final int id = 42;
    final GetRequest get = PowerMockito.mock(GetRequest.class);
    final RegionMovedException rme = new RegionMovedException("Boo!", get);
    when(get.deserialize(any(ByteBuf.class), anyInt()))
      .thenReturn(rme);
    when(get.getRegion()).thenReturn(region);
    rpcs_inflight.put(id, get);
    
    ByteBuf buffer = new ReadOnlyByteBuf(
        Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 1, 2, 8, 42 }));
    region_client.decode(ctx, buffer, null);

    assertEquals(0, rpcs_inflight.size());
    verify(hbase_client, times(1))
      .handleNSRE(get, region.name(), rme);
    verify(get, never()).callback(anyObject());
    verify(get, never()).getDeferred();
  }
  
  @Test
  public void regionMovedExceptionArrayBacked() throws Exception {
    final int id = 42;
    final GetRequest get = PowerMockito.mock(GetRequest.class);
    final RegionMovedException rme = new RegionMovedException("Boo!", get);
    when(get.deserialize(any(ByteBuf.class), anyInt()))
      .thenReturn(rme);
    when(get.getRegion()).thenReturn(region);
    rpcs_inflight.put(id, get);
    
    ByteBuf buffer = 
        Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 1, 2, 8, 42 });
    region_client.decode(ctx, buffer, null);

    assertEquals(0, rpcs_inflight.size());
    verify(hbase_client, times(1))
      .handleNSRE(get, region.name(), rme);
    verify(get, never()).callback(anyObject());
    verify(get, never()).getDeferred();
  }
  
  @Test (expected = IndexOutOfBoundsException.class)
  public void eom() throws Exception {
    // we read the whole message some how. Doesn't matter if it's array backed
    // or not in this case.
    final ByteBuf buffer = buildGoodResponse(true, 1);
    buffer.readerIndex(buffer.writerIndex());
    region_client.decode(ctx, buffer, null);
  }
  
  @Test
  public void notReadableInitial() throws Exception {
    // Fails on the first call to ensureReadable because the size encoded
    // in the first 4 bytes is much larger than it should be
    ByteBuf buffer = new ReadOnlyByteBuf(
        Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 42, 1 }));
    RuntimeException e = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected an IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException oob) {
      e = oob;
    }
    assertTrue(e instanceof IndexOutOfBoundsException);
    
    buffer = Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 42, 1 });
    e = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected an IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException oob) {
      e = oob;
    }
    assertTrue(e instanceof IndexOutOfBoundsException);
  }
  
  @Test
  public void negativeSize() throws Exception {
    // Fails on the first call to ensureReadable because the size encoded
    // in the first 4 bytes is much larger than it should be
    ByteBuf buffer = new ReadOnlyByteBuf(
        Unpooled.wrappedBuffer(new byte[] { -1, -1, -1, -1, 1 }));
    RuntimeException e = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      e = ex;
    }
    assertTrue(e instanceof IllegalArgumentException);
    
    buffer = Unpooled.wrappedBuffer(new byte[] { -1, -1, -1, -1, 1 });
    e = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      e = ex;
    }
    assertTrue(e instanceof IllegalArgumentException);
  }
  
  @Test
  public void rpcTooBig() throws Exception {
    // we only accept RPCs up to 256MBs in size right now. In order to avoid
    // allocating 256MB for unit testing, we'll tell the region client to
    // skip the ensureReadable call. Just over the line is 268435456 bytes
    // See HBaseRpc.MAX_BYTE_ARRAY_MASK
    
    PowerMockito.mockStatic(RegionClient.class);
    PowerMockito.doNothing().when(RegionClient.class, "ensureReadable", 
        any(ByteBuf.class), anyInt());
    ByteBuf buffer = new ReadOnlyByteBuf(
        Unpooled.wrappedBuffer(new byte[] { 16, 0, 0, 0, 1 }));
    RuntimeException e = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      e = ex;
    }
    assertTrue(e instanceof IllegalArgumentException);
    
    buffer = Unpooled.wrappedBuffer(new byte[] { 16, 0, 0, 0, 1 });
    e = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      e = ex;
    }
    assertTrue(e instanceof IllegalArgumentException);
  }
  
  @Test (expected = IndexOutOfBoundsException.class)
  public void nothingAfterInitialLength() throws Exception {
    // gets into HBaseRpc.readProtobuf and tosses an exception when it tries
    // to read the varint.
    final ByteBuf buffer = new ReadOnlyByteBuf(
        Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 0 }));
    region_client.decode(ctx, buffer, null);
  }
  
  @Test
  public void protobufVarintOnly() throws Exception {
    // gets into HBaseRpc.readProtobuf and tosses an exception when it tries
    // to readBytes on the non-array backed buffer
    ByteBuf buffer = new ReadOnlyByteBuf(
        Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 1, 1 }));
    RuntimeException e = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected an IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException oob) {
      e = oob;
    }
    assertTrue(e instanceof IndexOutOfBoundsException);
    
    buffer = Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 1, 1 });
    e = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected an IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException oob) {
      e = oob;
    }
    assertTrue(e instanceof IndexOutOfBoundsException);
  }

  @Test
  public void rpcNotInMap() throws Exception {
    // doesn't matter if the rest of the message is missing, we fail as
    // the ID isn't in the map. It also doesn't matter if it's negative since
    // the ID counter can rollover
    ByteBuf buffer = new ReadOnlyByteBuf(
        Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 1, 2, 8, 42 }));
    RuntimeException ex = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected a NonRecoverableException");
    } catch (RuntimeException e) {
      ex = e;
    }
    assertTrue(ex instanceof NonRecoverableException);
    
    buffer = new ReadOnlyByteBuf(
        Unpooled.wrappedBuffer(
            new byte[] { 0, 0, 0, 1, 6, 8, -42, -1, -1, -1, 15 }));
    ex = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected a NonRecoverableException");
    } catch (RuntimeException e) {
      ex = e;
    }
    assertTrue(ex instanceof NonRecoverableException);
    
    buffer = Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 1, 2, 8, 42 });
    ex = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected a NonRecoverableException");
    } catch (RuntimeException e) {
      ex = e;
    }
    assertTrue(ex instanceof NonRecoverableException);
    
    buffer = Unpooled.wrappedBuffer(
        new byte[] { 0, 0, 0, 1, 6, 8, -42, -1, -1, -1, 15 });
    ex = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected a NonRecoverableException");
    } catch (RuntimeException e) {
      ex = e;
    }
    assertTrue(ex instanceof NonRecoverableException);
  }

  @Test
  public void noCallId() throws Exception {
    // passes the header parsing since it has a size of zero, but then we check
    // to see if it has a call ID and it won't.
    ByteBuf buffer = new ReadOnlyByteBuf(
        Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 1, 0 }));
    RuntimeException e = null;
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected an NonRecoverableException");
    } catch (NonRecoverableException ex) {
      e = ex;
    }
    assertTrue(e instanceof NonRecoverableException);
    
    buffer = Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 1, 0 });
    try {
      region_client.decode(ctx, buffer, null);
      fail("Expected an NonRecoverableException");
    } catch (NonRecoverableException ex) {
      e = ex;
    }
    assertTrue(e instanceof NonRecoverableException);
  }

  @Test
  public void nullContext() throws Exception {
    // just shows we don't care about the context object
    final int id = 42;
    final ByteBuf buffer = buildGoodResponse(true, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    rpcs_inflight.put(id, get);
    
    region_client.decode(null, buffer, null);
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.joinUninterruptibly();
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
  }
  
  @Test (expected = IndexOutOfBoundsException.class)
  public void emptyBuffer() throws Exception {
    // This shouldn't happen since we should only get a buffer if the socket
    // had some data.
    final ByteBuf buf = Unpooled.wrappedBuffer(new byte[] {});
    region_client.decode(ctx, buf, null);
  }
  
  @Test (expected = NullPointerException.class)
  public void nullBuffer() throws Exception {
    // This should never happen, in theory
    region_client.decode(ctx, null, null);
  }

  @Test
  public void scratch() throws Exception {

    byte[] pkg = stringToBytes("0000000c0408d0e808060a0410001800000008f90408d4e808f2110a060a04080012000a0c0a04080112000a04080212000a060a04080312000a060a04080412000a060a04080512000a120a04080612000a04080712000a04080812000a060a04080912000a060a04080a12000a060a04080b12000a060a04080c12000a060a04080d12000a060a04080e12000a060a04080f12000a060a04081012000a060a04081112000a060a04081212000a060a04081312000a0c0a04081412000a04081512000a060a04081612000a0c0a04081712000a04081812000a060a04081912000a060a04081a12000a060a04081b12000a060a04081c12000a060a04081d12000a120a04081e12000a04081f12000a04082012000a060a04082112000a060a04082212000a060a04082312000a060a04082412000ae10712de070a316f72672e6170616368652e6861646f6f702e68626173652e4e6f7453657276696e67526567696f6e457863657074696f6e12a8076f72672e6170616368652e6861646f6f702e68626173652e4e6f7453657276696e67526567696f6e457863657074696f6e3a20526567696f6e2079616d61733a747364622c5c7831325c784243265c7843415c784237552c5c7844372c313433313434323137373635392e31306165653439303033363638383163333631353966633138346466646233612e206973206e6f74206f6e6c696e65206f6e20677372643236326e33342e7265642e79677269642e7961686f6f2e636f6d2c35303531312c313433363537333739393232330a096174206f72672e6170616368652e6861646f6f702e68626173652e726567696f6e7365727665722e48526567696f6e5365727665722e676574526567696f6e4279456e636f6465644e616d652848526567696f6e5365727665722e6a6176613a32393031290a096174206f72672e6170616368652e6861646f6f702e68626173652e726567696f6e7365727665722e48526567696f6e5365727665722e676574526567696f6e2848526567696f6e5365727665722e6a6176613a34343131290a096174206f72672e6170616368652e6861646f6f702e68626173652e726567696f6e7365727665722e48526567696f6e5365727665722e6d756c74692848526567696f6e5365727665722e6a6176613a33363234290a096174206f72672e6170616368652e6861646f6f702e68626173652e70726f746f6275662e67656e6572617465642e436c69656e7450726f746f7324436c69656e745365727669636524322e63616c6c426c6f636b696e674d6574686f6428436c69656e7450726f746f732e6a6176613a3239363039290a096174206f72672e6170616368652e6861646f6f702e68626173652e6970632e5270635365727665722e63616c6c285270635365727665722e6a6176613a32313534290a096174206f72672e6170616368652e6861646f6f702e68626173652e6970632e43616c6c52756e6e65722e72756e2843616c6c52756e6e65722e6a6176613a313038290a096174206f72672e6170616368652e6861646f6f702e68626173652e6970632e5270634578656375746f722e636f6e73756d65724c6f6f70285270634578656375746f722e6a6176613a313134290a096174206f72672e6170616368652e6861646f6f702e68626173652e6970632e5270634578656375746f7224312e72756e285270634578656375746f722e6a6176613a3934290a096174206a6176612e6c616e672e5468726561642e72756e285468726561642e6a6176613a373232290a0ae10712de070a316f72672e6170616368652e6861646f6f702e68626173652e4e6f7453657276696e67526567696f6e457863657074696f6e12a8076f72672e6170616368652e6861646f6f702e68626173652e4e6f7453657276696e67526567696f6e457863657074696f6e3a20526567696f6e2079616d61733a747364622c5c7831325c7844445c7841305c78413160555f5c7838412c313433363736353839363937382e64383065333930353837636632653832643937643231383134353431353137632e206973206e6f74206f6e6c696e65206f6e20677372643236326e33342e7265642e79677269642e7961686f6f2e636f6d2c35303531312c313433363537333739393232330a096174206f72672e6170616368652e6861646f6f702e68626173652e726567696f6e7365727665722e48526567696f6e5365727665722e676574526567696f6e4279456e636f6465644e616d652848526567696f6e5365727665722e6a6176613a32393031290a096174206f72672e6170616368652e6861646f6f702e68626173652e726567696f6e7365727665722e48526567696f6e5365727665722e676574526567696f6e2848526567696f6e5365727665722e6a6176613a34343131290a096174206f72672e6170616368652e6861646f6f702e68626173652e726567696f6e7365727665722e48526567696f6e5365727665722e6d756c74692848526567696f6e5365727665722e6a6176613a33363234290a096174206f72672e6170616368652e6861646f6f702e68626173652e70726f746f6275662e67656e6572617465642e436c69656e7450726f746f7324436c69656e745365727669636524322e63616c6c426c6f636b696e674d6574686f6428436c69656e7450726f746f732e6a6176613a3239363039290a096174206f72672e6170616368652e6861646f6f702e68626173652e6970632e5270635365727665722e63616c6c285270635365727665722e6a6176613a32313534290a096174206f72672e6170616368652e6861646f6f702e68626173652e6970632e43616c6c52756e6e65722e72756e2843616c6c52756e6e65722e6a6176613a313038290a096174206f72672e6170616368652e6861646f6f702e68626173652e6970632e5270634578656375746f722e636f6e73756d65724c6f6f70285270634578656375746f722e6a6176613a313134290a096174206f72672e6170616368652e6861646f6f702e68626173652e6970632e5270634578656375746f7224312e72756e285270634578656375746f722e6a6176613a3934290a096174206a6176612e6c616e672e5468726561642e72756e285468726561642e6a6176613a373232290a0a060a04082712000a060a0408281200");
    
    resetMockClient();
    
    final int id = 144468;
    
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    rpcs_inflight.put(144464, get);
    rpcs_inflight.put(id, get);

    region_client.channelRead(ctx, Unpooled.wrappedBuffer(Arrays.copyOf(pkg, 25)));
    region_client.channelRead(ctx, Unpooled.wrappedBuffer(Arrays.copyOfRange(pkg, 25, pkg.length)));
  }
  
  /**
   * Creates a simple GetRequest response with some dummy data and a single 
   * column in a row. The request lacks cell meta data.
   * @param array_backed Whether or not the Channel Buffer should have a backing
   * array or not to exercise zero-copy code paths
   * @param id The ID of the RPC that we're responding to.
   * @return A channel buffer with a ProtoBuf object as HBase would return
   * @throws IOException If we couldn't write the ProtoBuf for some reason
   */
  private ByteBuf buildGoodResponse(final boolean array_backed, final int id)
      throws IOException {
    final Cell cell = Cell.newBuilder()
        .setRow(Bytes.wrap(ROW))
        .setFamily(Bytes.wrap(FAMILY))
        .setQualifier(Bytes.wrap(QUALIFIER))
        .setTimestamp(TIMESTAMP)
        .setValue(Bytes.wrap(VALUE))
        .build();
    
    final Result result = Result.newBuilder()
        .addCell(cell)
        .build();
    
    final GetResponse get_response = 
        GetResponse.newBuilder()
        .setResult(result)
        .build();
    
    // TODO - test objects that return cell blocks, possibly scanners
//    final CellBlockMeta meta = CellBlockMeta.newBuilder()
//        .setLength(cell.getSerializedSize())
//        .build();
    
    final RPCPB.ResponseHeader header = RPCPB.ResponseHeader.newBuilder()
        .setCallId(id)
        //.setCellBlockMeta(meta)
        .build();
    
    final int hlen = header.getSerializedSize();
    final int vhlen = CodedOutputStream.computeRawVarint32Size(hlen);
    final int pblen = get_response.getSerializedSize();
    final int vlen = CodedOutputStream.computeRawVarint32Size(pblen);
    final byte[] buf = new byte[hlen + vhlen + vlen + pblen + 4];
    final CodedOutputStream out = CodedOutputStream.newInstance(buf, 4, 
        hlen + vhlen + vlen + pblen);
    
    out.writeMessageNoTag(header);
    out.writeMessageNoTag(get_response);
    
    Bytes.setInt(buf, buf.length - 4);
    if (array_backed) {
      return Unpooled.wrappedBuffer(buf);
    } else {
      return new ReadOnlyByteBuf(Unpooled.wrappedBuffer(buf));
    }
  }

  /** Simple test implementation of the HBaseException class */
  class TestingHBaseException extends HBaseException {
    private static final long serialVersionUID = 7717718589747017699L;
    TestingHBaseException(final String msg) {
      super(msg);
    }
  }

  /** Creates a new mock client that isn't spied. Necessary for the replay
   * tests
   */
  private void resetMockClient() throws Exception {
    region_client = new RegionClient(hbase_client);
    Whitebox.setInternalState(region_client, "chan", chan);
    Whitebox.setInternalState(region_client, "server_version", 
        RegionClient.SERVER_VERSION_095_OR_ABOVE);
    rpcs_inflight = Whitebox.getInternalState(
        region_client, "rpcs_inflight");
  }
  
  public static byte[] stringToBytes(final String bytes) {
    return DatatypeConverter.parseHexBinary(bytes);
  }
}