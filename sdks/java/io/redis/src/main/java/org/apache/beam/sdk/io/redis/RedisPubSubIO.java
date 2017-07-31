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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IO to use Redis PubSub.
 *
 * <h3>Reading from a Redis channel</h3>
 *
 * <p>RedisPubSubIO source returns an unbounded {@link PCollection} containing messages (as
 * {@code String}).
 *
 * <p>To configure a Redis PubSub source, you have to provide a Redis connection configuration
 * with a list of {@code host:port} to connect to the Redis server,
 * and your choice of channels or patterns where to subscribe. The following example illustrates
 * various options for configuring the source:
 *
 * <pre>{@code
 *
 *  pipeline.apply(
 *    RedisPubSubIO.read()
 *      .withConnection(RedisConnection.create(Collections.singletonList("localhost:6379")))
 *      .withChannels(Collections.singletonList("CHANNEL"))
 *
 * }</pre>
 *
 * <h3>Writing to a Redis channel</h3>
 *
 * <p>RedisPubSubIO sink supports writing {@code String} to a Redis channel.
 *
 * <p>To configure a Redis PubSub sink, as for the read, you have to specify a Redis connection
 * configuration with {@code host:port}. You can also provide the list of channels where to
 * publish messages.
 *
 * <p>For instance:
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(...) // provide PCollection<String>
 *    .apply(RedisPubSubIO.write()
 *      .withConnection(RedisConnection.create(Collections.singletonList("localhost:6379")))
 *      .withChannels(Collections.singletonList("CHANNEL"));
 *
 * }</pre>
 */
public class RedisPubSubIO {

  private static final Logger LOG = LoggerFactory.getLogger(RedisPubSubIO.class);

  public static Read read() {
    return new AutoValue_RedisPubSubIO_Read.Builder().setMaxReadTime(null)
        .setMaxNumRecords(Long.MAX_VALUE).build();
  }

  public static Write write() {
    return new AutoValue_RedisPubSubIO_Write.Builder().build();
  }

  private RedisPubSubIO() {
  }

  /**
   * A {@link PTransform} to read from Redis PubSub.
   */
  @AutoValue public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    @Nullable abstract RedisConnection connection();

    @Nullable abstract RedisService redisService();

    @Nullable abstract List<String> channels();

    @Nullable abstract List<String> patterns();

    abstract long maxNumRecords();

    @Nullable abstract Duration maxReadTime();

    abstract Builder builder();

    @AutoValue.Builder abstract static class Builder {

      abstract Builder setConnection(RedisConnection connection);

      abstract Builder setRedisService(RedisService redisService);

      abstract Builder setChannels(List<String> channels);

      abstract Builder setPatterns(List<String> patterns);

      abstract Builder setMaxNumRecords(long maxNumRecords);

      abstract Builder setMaxReadTime(Duration maxReadTime);

      abstract Read build();
    }

    /**
     * Define the connection to the Redis PubSub broker.
     *
     * @param connection The {@link RedisConnection} .
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withConnection(RedisConnection connection) {
      checkArgument(connection != null,
          "RedisPubSubIO.read().withConnection" + "(connection) called with null connection");
      return builder().setConnection(connection).build();
    }

    /**
     * Define the Redis service to use.
     *
     * @param redisService The {@link RedisService} to use.
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withRedisService(RedisService redisService) {
      checkArgument(redisService != null,
          "RedisPubSubIO.read().withRedisService(service) " + "called with null service");
      return builder().setRedisService(redisService).build();
    }

    /**
     * Define the list of Redis channels where the source subscribes.
     *
     * @param channels The list of channels.
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withChannels(List<String> channels) {
      checkArgument(channels != null,
          "RedisPubSubIO.read().withChannels(channels) called with " + "null channels");
      checkArgument(!channels.isEmpty(),
          "RedisPubSubIO.read().withChannels(channels) called " + "with empty channels list");
      return builder().setChannels(channels).build();
    }

    /**
     * Define the patterns used to select the channel.
     *
     * @param patterns The {@link List} of the channel patterns.
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withPatterns(List<String> patterns) {
      checkArgument(patterns != null,
          "RedisPubSubIO.read().withPatterns(patterns) called with " + "null patterns");
      checkArgument(!patterns.isEmpty(),
          "RedisPubSubIO.read().withPatterns(patterns) called " + "with empty patterns list");
      return builder().setPatterns(patterns).build();
    }

    /**
     * Define the max number of records to read from Redis PubSub.
     * When this max number of records is lower than {@code Long.MAX_VALUE}, the {@link Read}
     * will provide a bounded {@link PCollection}.
     *
     * @param maxNumRecords The max number of records to read from Redis PubSub.
     * @return The corresponding Read {@link PTransform}.
     */
    public Read withMaxNumRecords(long maxNumRecords) {
      checkArgument(maxReadTime() == null, "maxNumRecords and maxReadTime are exclusive");
      return builder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time on the Redis PubSub. When this max read time is defined, the
     * {@link Read} will provide a bounded {@link PCollection}.
     *
     * @param maxReadTime The max read time on the Redis PubSub.
     * @return The corresponding Read {@link PTransform}.
     */
    public Read withMaxReadTime(Duration maxReadTime) {
      checkArgument(maxNumRecords() == Long.MAX_VALUE,
          "maxNumRecords and maxReadTime are " + "exclusive");
      return builder().setMaxReadTime(maxReadTime).build();
    }

    @Override public PCollection<String> expand(PBegin input) {
      org.apache.beam.sdk.io.Read.Unbounded<String> unbounded = org.apache.beam.sdk.io.Read.from(
          new UnboundedRedisPubSubSource(this,
              new SerializableFunction<PipelineOptions, RedisService>() {

                @Override public RedisService apply(PipelineOptions input) {
                  return getRedisService(input);
                }
              }));

      PTransform<PBegin, PCollection<String>> transform = unbounded;

      if (maxNumRecords() != Long.MAX_VALUE) {
        transform = unbounded.withMaxNumRecords(maxNumRecords());
      } else if (maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime());
      }

      return input.getPipeline().apply(transform);
    }

    @Override public void validate(PipelineOptions options) {
      checkState(connection() != null || redisService() != null, "RedisPubSubIO.read() requires"
          + " a connection to be set via withConnection(connection) or a Redis service to be set "
          + "via withRedisService(service)");
      checkState(channels() != null || patterns() != null, "RedisPubSubIO.read() requires a "
          + "channel to be set via withChannels(channels) or a pattern to be set via withPatterns"
          + "(patterns)");
    }

    @Override public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      if (connection() != null) {
        connection().populateDisplayData(builder);
      }
      if (maxNumRecords() != Long.MAX_VALUE) {
        builder.add(DisplayData.item("maxNumRecords", maxNumRecords()));
      }
      builder.addIfNotNull(DisplayData.item("maxReadTime", maxReadTime()));
    }

    /**
     * Helper function to either get a fake/mock Redis service provided by
     * {@link #withRedisService(RedisService)} or creates and returns an implementation of a
     * concrete Redis service dealing with an actual Redis server.
     */
    @VisibleForTesting RedisService getRedisService(PipelineOptions pipelineOptions) {
      if (redisService() != null) {
        return redisService();
      }
      return new RedisServiceImpl(connection());
    }

  }

  private static class UnboundedRedisPubSubSource
      extends UnboundedSource<String, UnboundedSource.CheckpointMark> {

    private final Read spec;
    private final SerializableFunction<PipelineOptions, RedisService> serviceFactory;

    public UnboundedRedisPubSubSource(Read spec,
        SerializableFunction<PipelineOptions, RedisService> serviceFactory) {
      this.spec = spec;
      this.serviceFactory = serviceFactory;
    }

    @Override public UnboundedReader<String> createReader(PipelineOptions pipelineOptions,
        CheckpointMark checkpointMark) {
      return new UnboundedRedisPubSubReader(this, serviceFactory.apply(pipelineOptions));
    }

    @Override public List<UnboundedRedisPubSubSource> split(int desiredNumSplits,
        PipelineOptions pipelineOptions) {
      // Redis PubSub doesn't provide any dedup, so we create an unique subscriber
      return Collections.singletonList(new UnboundedRedisPubSubSource(spec, serviceFactory));
    }

    @Override public void validate() {
      spec.validate(null);
    }

    @Override public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
    }

    @Override public Coder getCheckpointMarkCoder() {
      return VoidCoder.of();
    }

    @Override public Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }

  }

  private static class UnboundedRedisPubSubReader extends UnboundedSource.UnboundedReader<String> {

    private final UnboundedRedisPubSubSource source;
    private final RedisService redisService;
    private RedisService.PubSubReader reader;

    private final BlockingQueue<String> queue;
    private String current;
    private Instant currentTimestamp;

    public UnboundedRedisPubSubReader(UnboundedRedisPubSubSource source,
        RedisService redisService) {
      this.source = source;
      this.redisService = redisService;
      this.queue = new LinkedBlockingQueue<>();
    }

    @Override public boolean start() throws IOException {
      reader = redisService
          .createPubSubReader(source.spec.channels(), source.spec.patterns(), queue);
      reader.start();
      return advance();
    }

    @Override public boolean advance() throws IOException {
      String message = queue.poll();
      if (message == null) {
        current = null;
        currentTimestamp = null;
        return false;
      }

      current = message;
      currentTimestamp = Instant.now();

      return true;
    }

    @Override public void close() throws IOException {
      reader.close();
      reader = null;
    }

    @Override public Instant getWatermark() {
      return currentTimestamp;
    }

    @Override public String getCurrent() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override public UnboundedRedisPubSubSource getCurrentSource() {
      return source;
    }

    @Override public Instant getCurrentTimestamp() {
      return currentTimestamp;
    }

    @Override public UnboundedSource.CheckpointMark getCheckpointMark() {
      return new UnboundedSource.CheckpointMark() {

        @Override public void finalizeCheckpoint() throws IOException {
          // nothing to do
        }
      };
    }

  }

  /**
   * A {@link PTransform} to publish messages to Redis PubSub channels.
   */
  @AutoValue public abstract static class Write extends PTransform<PCollection<String>, PDone> {

    @Nullable abstract RedisConnection connection();

    @Nullable abstract RedisService redisService();

    @Nullable abstract List<String> channels();

    abstract Builder builder();

    @AutoValue.Builder abstract static class Builder {

      abstract Builder setConnection(RedisConnection connection);

      abstract Builder setRedisService(RedisService redisService);

      abstract Builder setChannels(List<String> channels);

      abstract Write build();
    }

    /**
     * Define the connection to the Redis PubSub broker.
     *
     * @param connection The {@link RedisConnection}.
     * @return The corresponding {@link Write} {@link PTransform}.
     */
    public Write withConnection(RedisConnection connection) {
      checkArgument(connection != null,
          "RedisPubSubIO.write().withConnection" + "(connection) called with null connection");
      return builder().setConnection(connection).build();
    }

    /**
     * Define the Redis service to use.
     *
     * @param redisService The {@link RedisService} to use.
     * @return The corresponding {@link Write} {@link PTransform}.
     */
    public Write withRedisService(RedisService redisService) {
      checkArgument(redisService != null,
          "RedisPubSubIO.write().withRedisService(service) " + "called with null service");
      return builder().setRedisService(redisService).build();
    }

    /**
     * Define the pubsub channels to use.
     *
     * @param channels The list of the pubsub channels to use.
     * @return The corresponding {@link Write} {@link PTransform}.
     */
    public Write withChannels(List<String> channels) {
      checkArgument(channels != null,
          "RedisPubSubIO.write().withChannels(channels) called with" + " null channels");
      checkArgument(!channels.isEmpty(),
          "RedisPubSubIO.write().withChannels(channels) called " + "with empty channels list");
      return builder().setChannels(channels).build();
    }

    @Override public PDone expand(PCollection<String> input) {
      input.apply(
          ParDo.of(new WriteFn(this, new SerializableFunction<PipelineOptions, RedisService>() {

            @Override public RedisService apply(PipelineOptions input) {
              return getRedisService();
            }
          })));
      return PDone.in(input.getPipeline());
    }

    @Override public void validate(PipelineOptions options) {
      checkState(connection() != null || redisService() != null, "RedisPubSubIO.write() "
          + "requires a connection to be set via withConnection(connection) or a Redis service to"
          + "be set via withRedisService(service)");
      checkState(channels() != null && channels().size() > 0,
          "RedisPubSubIO.write() requires a" + " channel to be set via withChannels(channels)");
    }

    @Override public void populateDisplayData(DisplayData.Builder builder) {
      if (connection() != null) {
        connection().populateDisplayData(builder);
      }
    }

    RedisService getRedisService() {
      if (redisService() != null) {
        return redisService();
      }
      return new RedisServiceImpl(connection());
    }

    private static class WriteFn extends DoFn<String, Void> {

      private final Write spec;
      private final SerializableFunction<PipelineOptions, RedisService> serviceFactory;
      private RedisService.PubSubWriter writer;

      public WriteFn(Write spec,
          SerializableFunction<PipelineOptions, RedisService> serviceFactory) {
        this.spec = spec;
        this.serviceFactory = serviceFactory;
      }

      @StartBundle public void startBundle(StartBundleContext context) throws Exception {
        if (writer == null) {
          writer = serviceFactory.apply(context.getPipelineOptions())
              .createPubSubWriter(spec.channels());
          writer.start();
        }
      }

      @ProcessElement public void processElement(ProcessContext processContext) throws Exception {
        String element = processContext.element();
        writer.publish(element);
      }

      @Teardown public void teardown() throws Exception {
        writer.close();
        writer = null;
      }

    }

  }

}
