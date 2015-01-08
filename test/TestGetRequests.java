package org.hbase.async;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.stumbleupon.async.Callback;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.mockito.ArgumentMatcher;
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
final class TestGetRequests {

	private static final byte[] TIMESTAMP = "1234567890".getBytes();
	private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
	private static final byte[] KEY = { 'k', 'e', 'y' };
	private static final byte[] FAMILY = { 'f' };
	private static final byte[] QUALIFIER = { 'q', 'u', 'a', 'l' };
	private static final byte[] VALUE = { 'v', 'a', 'l', 'u', 'e' };
	private static final KeyValue KV = new KeyValue(KEY, FAMILY, QUALIFIER, VALUE);
	private static final RegionInfo meta = mkregion(".META.", ".META.,,1234567890");
	private static final RegionInfo region = mkregion("table", "table,,1234567890");

	private HBaseClient client = new HBaseClient("test-quorum-spec");
	/** Extracted from {@link #client}.  */
	private ConcurrentSkipListMap<byte[], RegionInfo> regions_cache;
	/** Extracted from {@link #client}.  */
	private ConcurrentHashMap<RegionInfo, RegionClient> region2client;
	/** Fake client supposedly connected to -ROOT-.  */
	private RegionClient rootclient = mock(RegionClient.class);
	/** Fake client supposedly connected to .META..  */
	private RegionClient metaclient = mock(RegionClient.class);
	/** Fake client supposedly connected to our fake test table.  */
	private RegionClient regionclient = mock(RegionClient.class);
	byte[] r;
	
	@Before
	public void before() throws Exception {
		Whitebox.setInternalState(client, "rootregion", rootclient);
		// Inject a timer that always fires away immediately.
		regions_cache = Whitebox.getInternalState(client, "regions_cache");
		region2client = Whitebox.getInternalState(client, "region2client");
		injectRegionInCache(meta, metaclient);
		injectRegionInCache(region, regionclient);
	}

	/**
	 * Injects an entry in the local META cache of the client.
	 */
	private void injectRegionInCache(final RegionInfo region,
			final RegionClient client) {
		regions_cache.put(region.name(), region);
		region2client.put(region, client);
		// We don't care about client2regions in these tests.
	}

	/**
	 * Test a simple get request with table and key
	 * @throws Exception
	 */
	@Test
	public void simpleGet() throws Exception {
		final GetRequest get = new GetRequest(TABLE, KEY);
		final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
		row.add(KV);

		when(regionclient.isAlive()).thenReturn(true);
		doAnswer(new Answer() {
			public Object answer(final InvocationOnMock invocation) {
				get.getDeferred().callback(row);
				return null;
			}
		}).when(regionclient).sendRpc(get);

		assertSame(row, client.get(get).joinUninterruptibly());
	}

	/**
	 * Test a get that queries by family byte
	 * @throws Exception
	 */
	@Test
	public void getWithFamily() throws Exception {
		final GetRequest get = new GetRequest(TABLE, KEY, FAMILY);
		final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
		row.add(KV);
		when(regionclient.isAlive()).thenReturn(true);
		doAnswer(new Answer() {
			public Object answer(final InvocationOnMock invocation) {
				get.getDeferred().callback(row);
				return null;
			}
		}).when(regionclient).sendRpc(get);
		assertSame(row, client.get(get).joinUninterruptibly());
	}
	
	/**
	 * Test a get that queries by family byte and qualifier byte
	 * @throws Exception
	 */
	@Test
	public void getWithQualifier() throws Exception {
		final GetRequest get = new GetRequest(TABLE, KEY, FAMILY, QUALIFIER);
		final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
		row.add(KV);
		when(regionclient.isAlive()).thenReturn(true);
		doAnswer(new Answer() {
			public Object answer(final InvocationOnMock invocation) {
				get.getDeferred().callback(row);
				return null;
			}
		}).when(regionclient).sendRpc(get);
		assertSame(row, client.get(get).joinUninterruptibly());
	}
	
	/**
	 * Test a get that queries by family string
	 * @throws Exception
	 */
	@Test
	public void getWithFamilyString() throws Exception{
		final GetRequest get = new GetRequest(new String(TABLE), new String(KEY), new String(FAMILY));
		final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
		row.add(KV);
		when(regionclient.isAlive()).thenReturn(true);
		doAnswer(new Answer() {
			public Object answer(final InvocationOnMock invocation) {
				get.getDeferred().callback(row);
				return null;
			}
		}).when(regionclient).sendRpc(get);
		assertSame(row, client.get(get).joinUninterruptibly());	
	}
	
	/**
	 * Test a get that queries by family string and qualifier string
	 * @throws Exception
	 */
	@Test
	public void getWithQualifierString() throws Exception {
		final GetRequest get = new GetRequest(new String(TABLE), new String(KEY), new String(FAMILY), new String(QUALIFIER));
		final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
		row.add(KV);
		when(regionclient.isAlive()).thenReturn(true);
		doAnswer(new Answer() {
			public Object answer(final InvocationOnMock invocation) {
				get.getDeferred().callback(row);
				return null;
			}
		}).when(regionclient).sendRpc(get);
		assertSame(row, client.get(get).joinUninterruptibly());	
	}
	
	
	// ----------------- //
	// Helper functions. //
	// ----------------- //

	private static <T> T getStatic(final String fieldname) {
		return Whitebox.getInternalState(HBaseClient.class, fieldname);
	}

	private static RegionInfo mkregion(final String table, final String name) {
		return new RegionInfo(table.getBytes(), name.getBytes(),
				HBaseClient.EMPTY_ARRAY);
	}

	
}
