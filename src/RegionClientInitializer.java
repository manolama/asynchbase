package org.hbase.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

public class RegionClientInitializer extends ChannelInitializer<SocketChannel>{
  private static final Logger LOG = LoggerFactory.getLogger(RegionClientInitializer.class);
  
  final HBaseClient hb_client;
  final RegionClient region_client;
  
  public RegionClientInitializer(final HBaseClient hb_client) {
    this.hb_client = hb_client;
    region_client = new RegionClient(hb_client);
  }
  
  @Override
  protected void initChannel(final SocketChannel ch) throws Exception {
    LOG.info("Initializing channel: " + ch);
    final ChannelPipeline pipeline = ch.pipeline();

    ChannelConfig config = ch.config();
    config.setOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
    config.setOption(ChannelOption.TCP_NODELAY, true);
    config.setOption(ChannelOption.SO_KEEPALIVE, true);
    // TODO - more options
    pipeline.addLast(region_client);
  }

}
