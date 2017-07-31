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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * Integration tests of {@link RedisIO}.
 *
 * <p>This test requires a running Redis. The Redis PubSub test doesn't require any data.
 * However the key/value store test require a the test dataset. The RedisTestDataset will create
 * the test key/value pairs.
 *
 * <p>You can run this test by doing the following:
 * <pre>
 *  mvn -e -Pio-it verify -pl sdks/java/io/redis -DintegrationTestPipelineOptions='[
 *  "--redisLocation=1.2.3.4:6379"]'
 * </pre>
 */
@RunWith(JUnit4.class) public class RedisIOIT {

  private static final Logger LOG = LoggerFactory.getLogger(RedisIOIT.class);

  @Rule public TestPipeline pipeline = TestPipeline.create();

  private static RedisTestOptions options;

  @BeforeClass public static void setup() throws Exception {
    PipelineOptionsFactory.register(RedisTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(RedisTestOptions.class);
  }

  @Test public void testEstimatedSizeBytes() throws Exception {
    Jedis jedis = RedisTestDataSet.getJedis(options);
    jedis.connect();
    for (int i = 0; i < 10; i++) {
      jedis.set("Foo " + i, "Bar " + i);
    }
    jedis.quit();

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    RedisIO.RedisSource source = new RedisIO.RedisSource("*",
        new SerializableFunction<PipelineOptions, RedisService>() {

          @Override public RedisService apply(PipelineOptions input) {
            return new RedisServiceImpl(
                RedisConnection.create(Collections.singletonList(options.getRedisLocation())));
          }
        });
    long estimatedSizeBytes = source.getEstimatedSizeBytes(pipelineOptions);
    LOG.info("Estimated size: {}", estimatedSizeBytes);
    assertEquals(100, estimatedSizeBytes);
  }

  @Test public void testGetRead() throws Exception {
    Jedis jedis = RedisTestDataSet.getJedis(options);
    RedisTestDataSet.createTestDataset(jedis);

    PCollection<KV<String, String>> output = pipeline.apply(RedisIO.read().withConnection(
        RedisConnection.create(Collections.singletonList(options.getRedisLocation()))));

    PAssert.thatSingleton(output.apply("Count", Count.<KV<String, String>>globally()))
        .isEqualTo(1000L);
    ArrayList<KV<String, String>> expected = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      KV<String, String> kv = KV.of("Key " + i, "Value " + i);
      expected.add(kv);
    }
    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();

    RedisTestDataSet.deleteTestDataset(jedis);
  }

  @Test public void testAppendWrite() throws Exception {
    Jedis jedis = RedisTestDataSet.getJedis(options);
    RedisTestDataSet.createTestDataset(jedis);

    ArrayList<KV<String, String>> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      KV<String, String> kv = KV.of("Key " + i, " Appended");
      data.add(kv);
    }
    pipeline.apply(Create.of(data)).apply(RedisIO.write().withConnection(
        RedisConnection.create(Collections.singletonList(options.getRedisLocation())))
        .withCommand(RedisIO.Write.Command.APPEND));
    pipeline.run();

    for (int i = 0; i < 1000; i++) {
      String value = jedis.get("Key " + i);
      assertEquals("Value " + i + " Appended", value);
    }

    RedisTestDataSet.deleteTestDataset(jedis);
  }

  @Test public void testSetWrite() throws Exception {
    ArrayList<KV<String, String>> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      KV<String, String> kv = KV.of("Key " + i, "Value " + i);
      data.add(kv);
    }
    pipeline.apply(Create.of(data)).apply(RedisIO.write().withConnection(
        RedisConnection.create(Collections.singletonList(options.getRedisLocation()))));
    pipeline.run();

    Jedis jedis = RedisTestDataSet.getJedis(options);
    for (int i = 0; i < 1000; i++) {
      String value = jedis.get("Key " + i);
      assertEquals("Value " + i, value);
    }

    RedisTestDataSet.deleteTestDataset(jedis);
  }

  @Test public void readPubSubChannelRead() throws Exception {
    final Jedis jedis = RedisTestDataSet.getJedis(options);
    Thread publisher = new Thread() {

      public void run() {
        try {
          Thread.sleep(1000);
          for (int i = 0; i < 1000; i++) {
            jedis.publish(options.getRedisPubSubChannel(), "Test " + i);
          }
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    };

    PCollection<String> output = pipeline.apply(RedisPubSubIO.read()
        .withConnection(RedisConnection.create(Arrays.asList(options.getRedisLocation())))
        .withChannels(Collections.singletonList(options.getRedisPubSubChannel()))
        .withMaxNumRecords(1000));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(1000L);

    ArrayList<String> expected = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      expected.add("Test " + i);
    }
    PAssert.that(output).containsInAnyOrder(expected);

    publisher.start();
    pipeline.run();
    publisher.join();
  }

  @Test public void testPubSubPatternRead() throws Exception {
    LOG.info("Creating publisher");
    final Jedis jedis = RedisTestDataSet.getJedis(options);
    Thread publisher = new Thread() {

      @Override public void run() {
        try {
          Thread.sleep(1000);
          for (int i = 0; i < 1000; i++) {
            jedis.publish("TEST_PATTERN", "Test " + i);
          }
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    };

    LOG.info("Creating test pipeline");
    PCollection<String> output = pipeline.apply(RedisPubSubIO.read().withConnection(
        RedisConnection.create(Collections.singletonList(options.getRedisLocation())))
        .withPatterns(Collections.singletonList("TEST_P*")).withMaxNumRecords(1000));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(1000L);

    ArrayList<String> expected = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      expected.add("Test " + i);
    }
    PAssert.that(output).containsInAnyOrder(expected);

    LOG.info("Starting publisher");
    publisher.start();
    LOG.info("Starting pipeline");
    pipeline.run();
    publisher.join();
  }

  @Test public void testPubSubWrite() throws Exception {
    final ArrayList<String> messages = new ArrayList<>();

    LOG.info("Starting Redis subscriber");
    final Jedis jedis = RedisTestDataSet.getJedis(options);
    Thread subscriber = new Thread() {

      @Override public void run() {
        LOG.info("Starting Redis subscriber thread");
        jedis.subscribe(new JedisPubSub() {

          @Override public void onMessage(String channel, String message) {
            messages.add(message);
            if (messages.size() == 1000) {
              unsubscribe();
            }
          }
        }, options.getRedisPubSubChannel());
      }
    };
    subscriber.start();

    LOG.info("Starting pipeline");
    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      data.add("Test " + i);
    }
    pipeline.apply(Create.of(data)).apply(RedisPubSubIO.write().withConnection(
        RedisConnection.create(Collections.singletonList(options.getRedisLocation())))
        .withChannels(Collections.singletonList(options.getRedisPubSubChannel())));
    pipeline.run();
    subscriber.join();

    assertEquals(1000, messages.size());
    for (int i = 0; i < 1000; i++) {
      assertTrue(messages.contains("Test " + i));
    }
  }

}
