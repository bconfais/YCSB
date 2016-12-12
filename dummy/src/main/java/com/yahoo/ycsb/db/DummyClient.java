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

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.Charset;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * Dummy YCSB binding.
 *
 * See {@code ipfs/README.md} for details.
 */
public class DummyClient extends DB {
  private String path;

  public static final String PATH_PROPERTY = "dummy.path";
  public static final String PATH_DEFAULT = "/tmp/";

  public void init() throws DBException {
    Properties props = getProperties();
    path = props.getProperty(PATH_PROPERTY);
    if (null == path) {
      path = PATH_DEFAULT;
    }
    System.out.println(path);
  }

  public void cleanup() throws DBException {
    path = null;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    Charset charset = Charset.forName("UTF-8");
    Path file = FileSystems.getDefault().getPath(path, key);
    try {
      result.put(key, new ByteArrayByteIterator(
          Files.readAllBytes(file)
      ));
    } catch (IOException e) {
      return new Status("ERROR-" + "", e.getMessage());
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    Charset charset = Charset.forName("UTF-8");
    Path file = FileSystems.getDefault().getPath(path, key);
    try (BufferedWriter writer = Files.newBufferedWriter(file, charset)) {
      writer.write(values.entrySet().iterator().next().getValue().toString());
    } catch (IOException e) {
      return new Status("ERROR-" + "", e.getMessage());
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
