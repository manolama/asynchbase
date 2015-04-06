/*
 * Copyright (C) 2015  The Async HBase Authors.  All rights reserved.
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
package org.hbase.async.auth;

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.sasl.SaslClient;

import org.hbase.async.HBaseClient;

/**
 * Handles simple user based authentication
 * @since 1.7
 */
public class SimpleClientAuthProvider extends ClientAuthProvider {
  
  /** Username to use for auth */
  private final String username;

  /**
   * Default ctor
   * @param hbase_client The HBaseClient to fetch configuration and timers from
   * @throws IllegalArgumentException if the 
   * asynchbase.security.auth.simple.username is missing, null or empty.
   */
  public SimpleClientAuthProvider(final HBaseClient hbase_client) {
    super(hbase_client);
    if (!hbase_client.getConfig()
        .hasProperty("asynchbase.security.auth.simple.username")) {
      throw new IllegalArgumentException("Missing client username");
    }
    username = hbase_client.getConfig()
        .getString("asynchbase.security.auth.simple.username");
    if (username == null || username.isEmpty()) {
      throw new IllegalArgumentException("Missing client username");
    }
  }

  @Override
  public SaslClient newSaslClient(final String service_ip, 
      final Map<String, String> props) {
    return null;
  }

  @Override
  public String getClientUsername() {
    return username;
  }

  @Override
  public byte getAuthMethodCode() {
    return ClientAuthProvider.SIMPLE_CLIENT_AUTH_CODE;
  }

  @Override
  public Subject getClientSubject() {
    return null;
  }

}
