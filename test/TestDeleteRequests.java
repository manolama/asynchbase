package org.hbase.async;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.stumbleupon.async.Callback;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.mockito.ArgumentMatcher;
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
import static org.powermock.api.mockito.PowerMockito.verifyNew;
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
	"ch.qos.*", "org.slf4j.*",
	"com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, RegionClient.class })
final class TestDeleteRequests {

	private static final byte[] TIMESTAMP = "1234567890".getBytes();
	private static final long TIMESTAMP_LONG = 1234567890;
	private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
	private static final byte[] KEY = { 'k', 'e', 'y' };
	private static final byte[] FAMILY = { 'f' };
	private static final byte[] QUALIFIER = { 'q', 'u', 'a', 'l' };
	private static final byte[][] QUALIFIERS = {{'q', 'u', 'a', 'l'},{'f', 'i' ,'e', 'r'}};
	private static final byte[] VALUE = { 'v', 'a', 'l', 'u', 'e' };
	private static final KeyValue KV = new KeyValue(KEY, FAMILY, QUALIFIER, VALUE);
	private static final RegionInfo meta = mkregion(".META.", ".META.,,1234567890");
	private static final RegionInfo region = mkregion("table", "table,,1234567890");
	private static final RowLock LOCK = new RowLock(region.name(), 1234567890);
	private static Boolean sendDelete; 

	/** Mocking out the HBaseClient.class so that we don't fire up all the netty threads*/
	private	HBaseClient client = mock(HBaseClient.class);
	/** Fake client supposedly connected to our fake test table.  */
	private RegionClient regionclient = mock(RegionClient.class);
	byte[] r;
	private final Counter num_deletes = new Counter();
	@Before
	public void before() throws Exception {
		sendDelete = false;
		Whitebox.setInternalState(client, "num_deletes", num_deletes);
	}

	/**
	 * Test a simple delete request
	 * @throws Exception
	 */
	@Test
	public void simpleDelete() throws Exception {
		final DeleteRequest delete = new DeleteRequest(TABLE, KEY);
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Test a delete that deletes a Key for a time
	 * @throws Exception
	 */
	@Test
	public void deleteWithTime() throws Exception {
		final DeleteRequest delete = new DeleteRequest(TABLE, KEY, TIMESTAMP_LONG);
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Test a delete that deletes based on family
	 * @throws Exception
	 */
	@Test
	public void deleteWithFamily() throws Exception {
		final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY);
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Test delete that deletes based on family and time
	 * @throws Exception
	 */
	@Test
	public void deleteWithFamilyAndTime() throws Exception {
		final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, TIMESTAMP_LONG);
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Test delete that deletes with qualifier
	 * @throws Exception
	 */
	@Test
	public void deleteWithQualifier() throws Exception {
		final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, QUALIFIER);
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Test delete that deletes with Qualifier and time
	 * @throws Exception
	 */
	@Test
	public void deleteWithQualifierAndTime() throws Exception {
		final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, QUALIFIER, TIMESTAMP_LONG);
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Test delete that deletes with multiple qualifiers
	 * @throws Exception
	 */
	@Test
	public void deleteWithColumn() throws Exception {
		final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, QUALIFIERS);
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Test delete that deletes with multiple qualifiers and time
	 * @throws Exception
	 */
	@Test
	public void deleteWithColumnAndTime() throws Exception {
		final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, QUALIFIERS, TIMESTAMP_LONG);
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Test delete that deletes with taking a row lock
	 * @throws Exception
	 */
	@Test
	public void deleteWithLock() throws Exception {
		final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, QUALIFIER, LOCK);
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Test delete that deletes with a specific row lock and time
	 * @throws Exception
	 */
	@Test
	public void deleteWithLockAndTime() throws Exception {
		final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, QUALIFIER, TIMESTAMP_LONG, LOCK);
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Test delete that deletes with multiple qualifiers and row lock
	 * @throws Exception
	 */
	@Test
	public void deleteWithColumnAndLock() throws Exception {
		final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, QUALIFIERS, LOCK);
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Test delete that takes input as Strings
	 * @throws Exception
	 */
	@Test
	public void deleteWithString() throws Exception {
		final DeleteRequest delete = new DeleteRequest(new String(TABLE), new String(KEY));
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Tests delete that takes family input as String
	 * @throws Exception
	 */
	@Test
	public void deleteWithFamilyString() throws Exception {
		final DeleteRequest delete = new DeleteRequest(new String(TABLE), new String(KEY), new String(FAMILY));
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Tests delete that takes qualifier input as String
	 * @throws Exception
	 */
	@Test
	public void deleteWithQualifierString() throws Exception {
		final DeleteRequest delete = new DeleteRequest(new String(TABLE), new String(KEY), new String(FAMILY), new String(QUALIFIER));
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Tests delete that takes input as String and a row lock
	 * @throws Exception
	 */
	@Test
	public void deleteWithStringLock() throws Exception {
		final DeleteRequest delete = new DeleteRequest(new String(TABLE), new String(KEY), new String(FAMILY), new String(QUALIFIER), LOCK);
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Tests delete that takes input KeyValue pair
	 * @throws Exception
	 */
	@Test
	public void deleteKV() throws Exception {
		final DeleteRequest delete = new DeleteRequest(TABLE, KV);
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}

	/**
	 * Tests delete that takes input KeyValue pair with a row lock
	 * @throws Exception
	 */
	@Test
	public void deleteKVWithLock() throws Exception {
		final DeleteRequest delete = new DeleteRequest(TABLE, KV, LOCK);
		stubbing(delete);
		client.delete(delete);
		Mockito.verify(client).sendRpcToRegion(delete);
		Mockito.verify(regionclient).sendRpc(delete);
		assertTrue(sendDelete);
	}
	// ----------------- //
	// Helper functions. //
	// ----------------- //

	private void stubbing(final DeleteRequest delete){
		when(regionclient.isAlive()).thenReturn(true);
		doAnswer(new Answer(){
			public Object answer(final InvocationOnMock invocation){
				delete.getDeferred().callback(null);
				sendDelete = true;
				return null;
			}
		}).when(regionclient).sendRpc(delete); 
		when(client.delete(delete)).thenCallRealMethod();

		doAnswer(new Answer(){
			public Object answer(final InvocationOnMock invocation){
				regionclient.sendRpc(delete);
				return null;
			}
		}).when(client).sendRpcToRegion(delete);
	}

	private static <T> T getStatic(final String fieldname) {
		return Whitebox.getInternalState(HBaseClient.class, fieldname);
	}

	private static RegionInfo mkregion(final String table, final String name) {
		return new RegionInfo(table.getBytes(), name.getBytes(),
				HBaseClient.EMPTY_ARRAY);
	}


}
