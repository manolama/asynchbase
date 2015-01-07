package org.hbase.async;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.stumbleupon.async.Callback;

import org.hbase.async.TestNSREs.FakeTimer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertTrue;

import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, RegionClient.class })
final class TestPutRequests {

  private static final long TIMESTAMP_LONG = 1234567890;
  private static final byte[] TIMESTAMP = "1234567890".getBytes();
  private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] KEY = { 'k', 'e', 'y' };
  private static final byte[] FAMILY = { 'f' };
  private static final byte[] QUALIFIER = { 'q', 'u', 'a', 'l' };
  private static final byte[][] QUALIFIERS = {{'q', 'u', 'a', 'l'},{'f', 'i' ,'e', 'r'}};
  private static final byte[] VALUE = { 'v', 'a', 'l', 'u', 'e' };
  private static final byte[][] VALUES = {{'v', 'a', 'l', 'u', 'e'},{'v', 'a', 'l', 'u', 'e', 's'}};
  private static final KeyValue KV = new KeyValue(KEY, FAMILY, QUALIFIER, VALUE);
  private static final RegionInfo meta = mkregion(".META.", ".META.,,1234567890");
  private static final RegionInfo region = mkregion("table", "table,,1234567890");
  private static final RowLock LOCK = new RowLock(region.name(), 1234567890);
  private static Boolean sendPut; 

  /** Mocking out the HBaseClient.class so that we don't fire up all the netty threads*/
  private HBaseClient client = mock(HBaseClient.class);
  /** Fake client supposedly connected to -ROOT-.  */
  private RegionClient rootclient = mock(RegionClient.class);
  /** Fake client supposedly connected to .META..  */
  private RegionClient metaclient = mock(RegionClient.class);
  /** Fake client supposedly connected to our fake test table.  */
  private RegionClient regionclient = mock(RegionClient.class);
  byte[] r;
  private final Counter num_puts = new Counter();
  @Before
  public void before() throws Exception {
    Whitebox.setInternalState(client, "rootregion", rootclient);
    sendPut = false;
	Whitebox.setInternalState(client, "num_puts", num_puts);
  }

 
 
  /**
   * Tests simple put that uses the current timestamp
   * @throws Exception
   */
  @Test
  public void simplePut() throws Exception {
	  final PutRequest put = new PutRequest(TABLE,KEY, FAMILY, QUALIFIER, VALUE);
	  stubbing(put);
		client.put(put);
		Mockito.verify(client).sendRpcToRegion(put);
		Mockito.verify(regionclient).sendRpc(put);
		assertTrue(sendPut);
  }
  
  /**
   * Test put request with specific timestamp
   * @throws Exception
   */
  @Test
  public void putWithTime() throws Exception {
	  final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE, TIMESTAMP_LONG);
	  stubbing(put);
		client.put(put);
		Mockito.verify(client).sendRpcToRegion(put);
		Mockito.verify(regionclient).sendRpc(put);
		assertTrue(sendPut);
  }
  
  /**
   * Test put that have multiple column qualifiers and values
   * @throws Exception
   */
  @Test
  public void putWithColumns() throws Exception {
	  final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIERS, VALUES, TIMESTAMP_LONG);
	  stubbing(put);
		client.put(put);
		Mockito.verify(client).sendRpcToRegion(put);
		Mockito.verify(regionclient).sendRpc(put);
		assertTrue(sendPut);
  }
  
  /**
   * Test put with a specific row lock
   * @throws Exception
   */
  @Test
  public void putWithLock() throws Exception {
	  final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE, LOCK);
	  stubbing(put);
		client.put(put);
		Mockito.verify(client).sendRpcToRegion(put);
		Mockito.verify(regionclient).sendRpc(put);
		assertTrue(sendPut);
  }
  
  /**
   * Test put with a specific timestamp and row lock
   * @throws Exception
   */
  @Test
  public void putWithLockAndTime() throws Exception {
	  final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE, TIMESTAMP_LONG, LOCK);
	  stubbing(put);
		client.put(put);
		Mockito.verify(client).sendRpcToRegion(put);
		Mockito.verify(regionclient).sendRpc(put);
		assertTrue(sendPut);
  }
  
  /**
   * Test put with multiple column qualifier, values and row lock
   * @throws Exception
   */
  @Test
  public void putWithColumnAndLock() throws Exception {
	  final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIERS, VALUES, TIMESTAMP_LONG, LOCK);
	  stubbing(put);
		client.put(put);
		Mockito.verify(client).sendRpcToRegion(put);
		Mockito.verify(regionclient).sendRpc(put);
		assertTrue(sendPut);
  }
  
  /**
   * Test put with strings instead of bytes.
   * @throws Exception
   */
  @Test
  public void putStrings() throws Exception {
	  final PutRequest put = new PutRequest(new String(TABLE), new String(KEY), new String(FAMILY), new String(QUALIFIER), new String(VALUE));
	  stubbing(put);
		client.put(put);
		Mockito.verify(client).sendRpcToRegion(put);
		Mockito.verify(regionclient).sendRpc(put);
		assertTrue(sendPut);
  }
  
  /**
   * Test put with KeyValue pair input
   * @throws Exception
   */
  @Test
  public void putKV() throws Exception{
	  final PutRequest put = new PutRequest(TABLE, KV);
	  stubbing(put);
		client.put(put);
		Mockito.verify(client).sendRpcToRegion(put);
		Mockito.verify(regionclient).sendRpc(put);
		assertTrue(sendPut);
  }
  
  
  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private void stubbing(final PutRequest put){
		when(regionclient.isAlive()).thenReturn(true);
		doAnswer(new Answer(){
			public Object answer(final InvocationOnMock invocation){
				put.getDeferred().callback(null);
				sendPut = true;
				return null;
			}
		}).when(regionclient).sendRpc(put); 
		when(client.put(put)).thenCallRealMethod();

		doAnswer(new Answer(){
			public Object answer(final InvocationOnMock invocation){
				regionclient.sendRpc(put);
				return null;
			}
		}).when(client).sendRpcToRegion(put);
	}
  private static <T> T getStatic(final String fieldname) {
    return Whitebox.getInternalState(HBaseClient.class, fieldname);
  }

  private static RegionInfo mkregion(final String table, final String name) {
    return new RegionInfo(table.getBytes(), name.getBytes(),
                          HBaseClient.EMPTY_ARRAY);
  }


}
