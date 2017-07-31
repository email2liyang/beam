/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test on the Redis IO.
 */
public class RedisIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(RedisIOTest.class);

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testEstimatedSizeBytes() throws Exception {
    final FakeRedisService service = new FakeRedisService();
    service.clear();
    for (int i = 0; i < 10; i++) {
      service.put("Foo " + i, "Bar " + i);
    }

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    RedisIO.RedisSource source = new RedisIO.RedisSource("Foo.*",
        new SerializableFunction<PipelineOptions, RedisService>() {
      @Override
      public RedisService apply(PipelineOptions input) {
        return service;
      }
    });
    long estimatedSizeBytes = source.getEstimatedSizeBytes(pipelineOptions);
    LOG.info("Estimated size: {}", estimatedSizeBytes);
    assertEquals(100, estimatedSizeBytes);
  }

  @Test
  public void testGetRead() throws Exception {
    FakeRedisService service = new FakeRedisService();
    for (int i = 0; i < 1000; i++) {
      service.put("Key " + i, "Value " + i);
    }

    PCollection<KV<String, String>> output =
        pipeline.apply(RedisIO.read().withRedisService(service));

    PAssert.thatSingleton(output.apply("Count", Count.<KV<String, String>>globally()))
        .isEqualTo(1000L);
    ArrayList<KV<String, String>> expected = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      KV<String, String> kv = KV.of("Key " + i, "Value " + i);
      expected.add(kv);
    }
    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void testSetWrite() throws Exception {
    FakeRedisService service = new FakeRedisService();

    ArrayList<KV<String, String>> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      KV<String, String> kv = KV.of("Key " + i, "Value " + i);
      data.add(kv);
    }
    pipeline.apply(Create.of(data))
        .apply(RedisIO.write().withRedisService(service));
    pipeline.run();

    for (int i = 0; i < 1000; i++) {
      String value = service.get("Key " + i);
      assertEquals("Value " + i, value);
    }
  }

  @Test
  public void testAppendWrite() throws Exception {
    FakeRedisService service = new FakeRedisService();

    for (int i = 0; i < 1000; i++) {
      service.put("Key " + i, "Value " + i);
    }

    ArrayList<KV<String, String>> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      KV<String, String> kv = KV.of("Key " + i, " Appended");
      data.add(kv);
    }
    pipeline.apply(Create.of(data))
        .apply(RedisIO.write()
            .withRedisService(service)
            .withCommand(RedisIO.Write.Command.APPEND));
    pipeline.run();

    for (int i = 0; i < 1000; i++) {
      String value = service.get("Key " + i);
      assertEquals("Value " + i +  " Appended", value);
    }
  }

  @Test
  public void testPubSubRead() throws Exception {
    final FakeRedisService redisService = new FakeRedisService();

    final FakeRedisService.FakeRedisPubSubWriter publisher = redisService.createPubSubWriter(null);
    publisher.start();
    Thread publisherThread = new Thread() {
      @Override
      public void run() {
        try {
          while (redisService.getPubSubReadersCount() == 0) {
            Thread.sleep(200);
          }
          for (int i = 0; i < 1000; i++) {
            publisher.publish("Test " + i);
          }
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    };

    PCollection<String> output = pipeline.apply(RedisPubSubIO.read()
        .withRedisService(redisService)
        .withChannels(Collections.singletonList("TEST"))
        .withMaxNumRecords(1000));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(1000L);

    ArrayList<String> expected = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      expected.add("Test " + i);
    }
    PAssert.that(output).containsInAnyOrder(expected);

    publisherThread.start();
    pipeline.run();
    publisherThread.join();

    publisher.close();
  }

  @Test
  public void testPubSubWrite() throws Exception {
    FakeRedisService redisService = new FakeRedisService();

    final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    FakeRedisService.FakeRedisPubSubReader subscriber = redisService.createPubSubReader(null,
        null, queue);
    subscriber.start();

    final ArrayList<String> messages = new ArrayList<>();

    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      data.add("Test " + i);
    }
    pipeline.apply(Create.of(data))
        .apply(RedisPubSubIO.write()
            .withRedisService(redisService)
            .withChannels(Collections.singletonList("TEST")));
    pipeline.run();

    String record = null;
    while ((record = queue.poll()) != null) {
      messages.add(record);
    }

    assertEquals(1000, messages.size());
    for (int i = 0; i < 1000; i++) {
      assertTrue(messages.contains("Test " + i));
    }

    subscriber.close();
  }

  /**
   * A {@link RedisService} implementation that stores key/value pairs in memory.
   */
  private static class FakeRedisService implements RedisService {

    private static final Map<String, String> store = new HashMap<>();
    private static final List<FakeRedisPubSubReader> pubSubReaders = new ArrayList<>();

    public void put(String key, String value) {
      store.put(key, value);
    }

    public String get(String key) {
      return store.get(key);
    }

    public void clear() {
      store.clear();
    }

    @Override
    public FakeRedisReader createReader(String keyPattern) throws IOException {
      return new FakeRedisReader(keyPattern);
    }

    static class FakeRedisReader implements Reader {

      private final String keyPattern;
      private Iterator<String> keysIterator;
      private KV<String, String> current;

      public FakeRedisReader(String keyPattern) {
        this.keyPattern = keyPattern;
      }

      @Override
      public boolean start() {
        keysIterator = store.keySet().iterator();
        return advance();
      }

      @Override
      public boolean advance() {
        if (keysIterator.hasNext()) {
          String key = keysIterator.next();
          if (keyPattern != null && !keyPattern.equals("*")) {
            if (!key.matches(keyPattern)) {
              return false;
            }
          }
          String value = store.get(key);
          KV<String, String> record = KV.of(key, value);
          current = record;
          return true;
        }
        return false;
      }

      @Override
      public void close() {
        keysIterator = null;
        current = null;
      }

      @Override
      public KV<String, String> getCurrent() {
        if (current == null) {
          throw new NoSuchElementException();
        }
        return current;
      }

    }

    @Override
    public long getEstimatedSizeBytes() {
      long size = 0L;
      Iterator<String> keysIterator = store.keySet().iterator();
      long count = 0;
      while (keysIterator.hasNext() && count < 10) {
        count++;
        String key = keysIterator.next();
        String value = store.get(key);
        size += key.getBytes().length + value.getBytes().length;
      }
      return size;
    }

    @Override
    public FakeRedisWriter createWriter() throws IOException {
      return new FakeRedisWriter();
    }

    static class FakeRedisWriter implements Writer {

      @Override
      public void start() {
        // nothing to do
      }

      @Override
      public void write(KV<String, String> record, RedisIO.Write.Command command) {
        if (command == RedisIO.Write.Command.SET) {
          store.put(record.getKey(), record.getValue());
        } else if (command == RedisIO.Write.Command.APPEND) {
          String value = store.get(record.getKey());
          if (value != null) {
            value = value + record.getValue();
          } else {
            value = record.getValue();
          }
          store.put(record.getKey(), value);
        } else {
          throw new IllegalArgumentException("Command " + command + " not supported");
        }
      }

      @Override
      public void close() {
        // nothing to do
      }

    }

    public int getPubSubReadersCount() {
      return pubSubReaders.size();
    }

    class FakeRedisPubSubReader implements PubSubReader {

      private final BlockingQueue<String> queue;

      public FakeRedisPubSubReader(BlockingQueue<String> queue) {
        this.queue = queue;
      }

      @Override
      public void start() {
        // register in the readers
        pubSubReaders.add(this);
      }

      @Override
      public void close() {
        // unregister from the readers
        pubSubReaders.remove(this);
      }

      public void callback(String record) throws Exception {
        queue.put(record);
      }

    }

    public FakeRedisPubSubReader createPubSubReader(List<String> channels, List<String> patterns,
                                    BlockingQueue<String> queue) {
      return new FakeRedisPubSubReader(queue);
    }

    static class FakeRedisPubSubWriter implements PubSubWriter {

      private final Logger log = LoggerFactory.getLogger(FakeRedisPubSubWriter.class);

      @Override
      public void start() {
        // nothing to do
      }

      @Override
      public void publish(String record) {
        for (FakeRedisPubSubReader reader : pubSubReaders) {
          try {
            reader.callback(record);
          } catch (Exception e) {
            log.error("Can't publish record", e);
          }
        }
      }

      @Override
      public void close() {
        // nothing to do
      }

    }

    @Override
    public FakeRedisPubSubWriter createPubSubWriter(List<String> channels) {
      return new FakeRedisPubSubWriter();
    }
  }

}
