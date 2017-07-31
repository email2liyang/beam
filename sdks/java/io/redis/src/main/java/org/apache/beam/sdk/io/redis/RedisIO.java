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

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IO to manipulate Redis key/value database.
 *
 * <h3>Reading Redis key/value pairs</h3>
 *
 * <p>RedisIO.Read provides a source which returns a bounded {@link PCollection} containing
 * key/value pairs as {@code KV<String, String>}.
 *
 * <p>To configure a Redis source, you have to provide Redis server hostname and port number.
 * Optionally, you can provide a key pattern (to filter the keys). The following example
 * illustrates how to configure a source:
 *
 * <pre>{@code
 *
 *  pipeline.apply(RedisIO.read()
 *    .withConnection(RedisConnection.create(Collections.singletonList("localhost:6379")))
 *    .withKeyPattern("foo*")
 *
 * }</pre>
 *
 * <h3>Writing Redis key/value pairs</h3>
 *
 * <p>RedisIO.Write provides a sink to write key/value pairs into a Redis database.
 *
 * <p>To configure a Redis sink, as for the source, you have to provide Redis server hostname and
 * port number. Optionally, you can provide the Redis command to set/update the key/value pairs.
 * The following example illustrates how to configure a sink:
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(...) // provide PCollection<KV<String, String>>
 *    .apply(RedisIO.write()
 *      .withConnection(RedisConnection.create(Collections.singletonList("localhost:6379")))
 *
 * }</pre>
 */
public class RedisIO {

  private static final Logger LOG = LoggerFactory.getLogger(RedisIO.class);

  public static Read read() {
    return new AutoValue_RedisIO_Read.Builder().setCommand(Read.Command.GET).setKeyPattern("*")
        .build();
  }

  public static Write write() {
    return new AutoValue_RedisIO_Write.Builder().setCommand(Write.Command.SET).build();
  }

  private RedisIO() {
  }

  /**
   * A {@link PTransform} reading key/value pairs from a Redis database.
   */
  @AutoValue public abstract static class Read
      extends PTransform<PBegin, PCollection<KV<String, String>>> {

    /**
     * The Redis commands related to read of key-value pairs.
     */
    public enum Command {
      GET
    }

    @Nullable abstract RedisConnection connection();

    @Nullable abstract Command command();

    @Nullable abstract String keyPattern();

    @Nullable abstract RedisService redisService();

    abstract Builder builder();

    @AutoValue.Builder abstract static class Builder {

      @Nullable abstract Builder setConnection(RedisConnection connection);

      @Nullable abstract Builder setCommand(Command command);

      @Nullable abstract Builder setKeyPattern(String keyPattern);

      @Nullable abstract Builder setRedisService(RedisService redisService);

      abstract Read build();
    }

    /**
     * Define the connection to the Redis server.
     *
     * @param connection The {@link RedisConnection}.
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withConnection(RedisConnection connection) {
      checkArgument(connection != null,
          "RedisIO.read().withConnection(connection) called with " + "null connection");
      return builder().setConnection(connection).build();
    }

    /**
     * Define the Redis command to execute to retrieve the key/value pairs.
     *
     * @param command The Redis command.
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withCommand(Command command) {
      checkArgument(command != null,
          "RedisIO.read().withCommand(command) called with null " + "command");
      return builder().setCommand(command).build();
    }

    /**
     * Define the pattern to filter the Redis keys.
     *
     * @param keyPattern The filter key pattern.
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withKeyPattern(String keyPattern) {
      checkArgument(keyPattern != null,
          "RedisIO.read().withKeyPattern(pattern) called with " + "null pattern");
      return builder().setKeyPattern(keyPattern).build();
    }

    /**
     * Define the Redis service to use.
     *
     * @param redisService The {@link RedisService} to use.
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withRedisService(RedisService redisService) {
      checkArgument(redisService != null,
          "RedisIO.read().withRedisService(service) called with" + " null service");
      return builder().setRedisService(redisService).build();
    }

    @Override public void validate(PipelineOptions options) {
      checkState(connection() != null || redisService() != null, "RedisIO.read() requires a "
          + "connection to be set withConnection(connection) or a service to be set "
          + "withRedisService(service)");
      checkState(command() != null,
          "RedisIO.read() requires a command to be set via " + "withCommand(command)");
      checkState(keyPattern() != null,
          "RedisIO.read() requires a key pattern to be set via " + "withKeyPattern(keyPattern");
    }

    @Override public void populateDisplayData(DisplayData.Builder builder) {
      if (connection() != null) {
        connection().populateDisplayData(builder);
      }
      builder.addIfNotNull(DisplayData.item("command", command().toString()));
    }

    @Override public PCollection<KV<String, String>> expand(PBegin input) {
      return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(
          new RedisSource(keyPattern(), new SerializableFunction<PipelineOptions, RedisService>() {

            @Override public RedisService apply(PipelineOptions pipelineOptions) {
              return getRedisService(pipelineOptions);
            }
          })));
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

  /**
   * A bounded source reading key-value pairs from a Redis server.
   */
  // visible for testing
  protected static class RedisSource extends BoundedSource<KV<String, String>> {

    private final String keyPattern;
    private final SerializableFunction<PipelineOptions, RedisService> serviceFactory;

    protected RedisSource(String keyPattern,
        SerializableFunction<PipelineOptions, RedisService> serviceFactory) {
      this.keyPattern = keyPattern;
      this.serviceFactory = serviceFactory;
    }

    @Override public List<RedisSource> split(long desiredBundleSizeBytes,
        PipelineOptions pipelineOptions) {
      // TODO cluster with one source per slot
      return Collections.singletonList(this);
    }

    /**
     * The estimate size bytes is based on sampling, computing average size of 10 random
     * key/value pairs. This sampling average size is used with the Redis dbSize to get an
     * estimation of the actual database size.
     *
     * @param pipelineOptions The pipeline options.
     * @return The estimated size of the Redis database in bytes.
     */
    @Override public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws Exception {
      return serviceFactory.apply(pipelineOptions).getEstimatedSizeBytes();
    }

    @Override public BoundedReader<KV<String, String>> createReader(
        PipelineOptions pipelineOptions) {
      return new RedisReader(this, serviceFactory.apply(pipelineOptions));
    }

    @Override public void validate() {
      // done in the Read
    }

    @Override public Coder<KV<String, String>> getDefaultOutputCoder() {
      return KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
    }

  }

  private static class RedisReader extends BoundedSource.BoundedReader<KV<String, String>> {

    private final RedisSource source;
    private final RedisService service;
    private RedisService.Reader reader;

    public RedisReader(RedisSource source, RedisService service) {
      this.source = source;
      this.service = service;
    }

    @Override public boolean start() throws IOException {
      reader = service.createReader(source.keyPattern);
      return reader.start();
    }

    @Override public boolean advance() throws IOException {
      return reader.advance();
    }

    @Override public void close() throws IOException {
      reader.close();
      reader = null;
    }

    @Override public KV<String, String> getCurrent() {
      return reader.getCurrent();
    }

    @Override public RedisSource getCurrentSource() {
      return source;
    }

  }

  /**
   * A {@link PTransform} to store or update key/value pair in a Redis database.
   */
  @AutoValue public abstract static class Write
      extends PTransform<PCollection<KV<String, String>>, PDone> {

    /**
     * Redis commands related to store of key-value pairs.
     */
    public enum Command {
      SET, APPEND, SETNX
    }

    @Nullable abstract RedisConnection connection();

    @Nullable abstract Command command();

    @Nullable abstract RedisService redisService();

    abstract Builder builder();

    @AutoValue.Builder abstract static class Builder {

      abstract Builder setConnection(RedisConnection connection);

      abstract Builder setCommand(Command command);

      abstract Builder setRedisService(RedisService redisService);

      abstract Write build();
    }

    /**
     * Define the connection to the Redis server.
     *
     * @param connection The Redis server connection configuration.
     * @return The corresponding {@link Write} {@link PTransform}.
     */
    public Write withConnection(RedisConnection connection) {
      checkArgument(connection != null,
          "RedisIO.write().withConnection(connection) called with" + " null connection");
      return builder().setConnection(connection).build();
    }

    /**
     * Define the command to use to store the record in the Redis server.
     *
     * @param command Redis command to use (SET, APPEND, ...).
     * @return The corresponding {@link Write} {@link PTransform}.
     */
    public Write withCommand(Command command) {
      checkArgument(command != null,
          "RedisIO.write().withCommand(command) called with null " + "command");
      return builder().setCommand(command).build();
    }

    /**
     * Define the Redis service to use.
     *
     * @param redisService The {@link RedisService} to use.
     * @return The corresponding {@link Write} {@link PTransform}.
     */
    public Write withRedisService(RedisService redisService) {
      checkArgument(redisService != null,
          "RedisIO.write().withRedisService(service) called " + "with null service");
      return builder().setRedisService(redisService).build();
    }

    @Override public PDone expand(PCollection<KV<String, String>> input) {
      input.apply(
          ParDo.of(new WriteFn(this, new SerializableFunction<PipelineOptions, RedisService>() {

            @Override public RedisService apply(PipelineOptions input) {
              return getRedisService(input);
            }
          })));
      return PDone.in(input.getPipeline());
    }

    @Override public void validate(PipelineOptions options) {
      checkState(command() != null, "Command is null");
    }

    @Override public void populateDisplayData(DisplayData.Builder builder) {
      if (connection() != null) {
        connection().populateDisplayData(builder);
      }
      builder.addIfNotNull(DisplayData.item("command", command().toString()));
    }

    /**
     * Helper function that either returns the mock Redis service supplied by
     * {@link #withRedisService(RedisService)} or creates and returns a concrete implementation
     * communicating with Redis server.
     */
    RedisService getRedisService(PipelineOptions options) {
      if (redisService() != null) {
        return redisService();
      }
      return new RedisServiceImpl(connection());
    }

    private static class WriteFn extends DoFn<KV<String, String>, Void> {

      private final Write spec;
      private final SerializableFunction<PipelineOptions, RedisService> redisServiceFactory;
      private RedisService.Writer writer;

      public WriteFn(Write spec,
          SerializableFunction<PipelineOptions, RedisService> redisServiceFactory) {
        this.spec = spec;
        this.redisServiceFactory = redisServiceFactory;
      }

      @StartBundle public void startBundle(StartBundleContext context) throws Exception {
        if (writer == null) {
          writer = redisServiceFactory.apply(context.getPipelineOptions()).createWriter();
          writer.start();
        }
      }

      @ProcessElement public void processElements(ProcessContext processContext) throws Exception {
        KV<String, String> element = processContext.element();
        writer.write(element, spec.command());
      }

      @Teardown public void teardown() throws Exception {
        writer.close();
      }

    }

  }

}
