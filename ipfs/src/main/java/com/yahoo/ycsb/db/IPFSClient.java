/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import org.ipfs.api.IPFS;
import org.ipfs.api.IPFS.Block;
import org.ipfs.api.MerkleNode;
import org.ipfs.api.Multihash;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.Charset;
import java.nio.file.StandardOpenOption;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONObject;

/**
 * YCSB binding for <a href="http://ipfs.io/">IPFS</a>.
 *
 * See {@code ipfs/README.md} for details.
 */
public class IPFSClient extends DB {
  public static final int NB_HOSTS = 4;
  public static final String HOST_PROPERTY = "ipfs.host";
  public static final String PORT_PROPERTY = "ipfs.port";
  public static final String HOST_DEFAULT = "127.0.0.1";
  public static final String PORT_DEFAULT = "5001";

  private static IPFS[] ipfs = new IPFS[NB_HOSTS];
  private static Random rand;

  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  public void init() throws DBException {
    long s = 0;
    long ss = 0;
    final int curInitCount = INIT_COUNT.incrementAndGet();
    synchronized(INIT_COUNT) {
      if(1<curInitCount) {
        return;
      }
      s = System.nanoTime();
      Properties props = getProperties();
      String[] hosts = new String[NB_HOSTS];
      String[] ports = new String[NB_HOSTS];
      rand = new Random();
      for (int i = 0; i < NB_HOSTS; i++) {
        hosts[i] = props.getProperty(HOST_PROPERTY+(i+1));
        ports[i] = props.getProperty(PORT_PROPERTY+(i+1));
        if (null == hosts[i]) {
          hosts[i] = HOST_DEFAULT;
        }
        if (null == ports[i]) {
          ports[i] = PORT_DEFAULT;
        }
      }
      // TODO: try catch
      for (int i = 0; i < NB_HOSTS; i++) {
        System.out.println(hosts[i]+"/"+ports[i]);
        ipfs[i] = new IPFS("/ip4/"+hosts[i]+"/tcp/"+ports[i]);
      }
    }
    ss = System.nanoTime();
    System.out.println("connect: "+(ss-s)+" - "+Thread.currentThread().getName());
  }

  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();
      if (curInitCount <= 0) {
        for (int i = 0; i < NB_HOSTS; i++) {
          ipfs[i] = null;
        }
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    String hash = null;
    try {
      List<String> lines = Files.readAllLines(Paths.get("/tmp/ids"), Charset.forName("UTF-8"));
      for (String line : lines) {
        String[] parts = line.split(" ");
        if (parts[0].equals(key)) {
          hash = parts[1];
          break;
        }
      }
    } catch(IOException e) {
      return new Status("ERROR-" + "", e.getMessage());
    }
    if (null == hash) {
      return new Status("ERROR-" + "", "no key");
    }
    // select a server
    int s = rand.nextInt(100);

    Block b = ipfs[s%NB_HOSTS].new Block();
    try {
      byte[] data = b.get(Multihash.fromBase58(hash));
      result.put(key, new ByteArrayByteIterator(data));
    } catch(IOException e) {
      return new Status("ERROR-" + "", e.getMessage());
    } catch(Exception e) {
      return new Status("ERROR-" + "", e.getMessage());
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    JSONObject json = new JSONObject();
    for (final Entry<String, ByteIterator> e : values.entrySet()) {
      json.put(e.getKey(), e.getValue().toString());
    }

    // select a server
    int s = rand.nextInt(100);

    Block b = ipfs[s%NB_HOSTS].new Block();
    List<byte[]> a = new ArrayList<byte[]>();
    a.add(json.toString().getBytes());
    String hash = null;
    try {
      MerkleNode merkle = b.put(a).get(0);
      hash = merkle.hash.toString();
    } catch(IOException e) {
      return new Status("ERROR-" + "", e.getMessage());
    }

    if (null != hash) {
      try {
        String line = new String();
        line = key.toString()+" "+hash.toString()+"\n";
        Files.write(Paths.get("/tmp/ids"), line.getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
      } catch(IOException e) {
        return new Status("ERROR-" + "", e.getMessage());
      }
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}
