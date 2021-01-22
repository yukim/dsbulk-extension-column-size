package com.datastax.oss.dsbulk.extention.columnsize.workflow;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import com.datastax.oss.dsbulk.connectors.api.DefaultRecord;
import com.datastax.oss.dsbulk.connectors.api.Field;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.workflow.commons.schema.ReadResultMapper;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import reactor.core.publisher.Flux;

/** Filter out rows that has the column that its size is more than threshold */
public class DetectRowWithLargeSize implements ReadResultMapper {

  private static final String COLUMN_PREFIX = "size_of__";

  private static final Set<Integer> INCLUSION_TYPES =
      IntStream.of(
              ProtocolConstants.DataType.ASCII,
              ProtocolConstants.DataType.BLOB,
              ProtocolConstants.DataType.CUSTOM,
              ProtocolConstants.DataType.LIST,
              ProtocolConstants.DataType.MAP,
              ProtocolConstants.DataType.SET,
              ProtocolConstants.DataType.UDT,
              ProtocolConstants.DataType.TUPLE,
              ProtocolConstants.DataType.VARCHAR)
          .boxed()
          .collect(Collectors.toSet());

  private final RelationMetadata schema;
  private final long columnSizeThreshold;

  private final ConcurrentMap<String, Histogram> columnSizeStats = new ConcurrentHashMap<>();
  private final LongAdder totalRowCounter = new LongAdder();
  private final LongAdder aboveThresholdCounter = new LongAdder();

  /** @param columnSizeThreshold column size threshold in bytes. */
  public DetectRowWithLargeSize(RelationMetadata tableSchema, long columnSizeThreshold) {
    this.schema = tableSchema;
    this.columnSizeThreshold = columnSizeThreshold;
  }

  @NonNull
  @Override
  public Record map(@NonNull ReadResult result) {
    Row row = result.getRow().orElseThrow(IllegalStateException::new);
    URI uri =
        URI.create(
            "cql://" + schema.getKeyspace().asInternal() + '/' + schema.getName().asInternal());
    DefaultRecord record = new DefaultRecord(null, uri, -1);
    for (int i = 0; i < row.size(); i++) {
      ColumnDefinition col = row.getColumnDefinitions().get(i);
      // if the column is part of primary key, store the value as it is
      if (schema.getPrimaryKey().stream()
          .map(ColumnMetadata::getName)
          .anyMatch(name -> name.equals(col.getName()))) {
        TypeCodec<?> codec = DefaultCodecRegistry.DEFAULT.codecFor(col.getType());
        record.put(new DefaultMappedField(col.getName().asCql(true)), row.get(i, codec));
      } else {
        // regular columns with the type of variable byte size (text, blob, etc)
        // only record the size of column
        if (INCLUSION_TYPES.contains(col.getType().getProtocolCode())) {
          ByteBuffer bb = row.getBytesUnsafe(i);
          if (bb != null) {
            CqlIdentifier nameWithPrefix =
                CqlIdentifier.fromInternal(COLUMN_PREFIX + col.getName().asInternal());
            int size = bb.limit() - bb.position();
            record.put(new DefaultMappedField(nameWithPrefix.asCql(true)), size);
          }
        }
      }
    }
    totalRowCounter.increment();
    return record;
  }

  public Flux<Record> countFiltered(Flux<Record> recordFlux) {
    return recordFlux.doOnNext((record) -> aboveThresholdCounter.increment());
  }

  @SuppressWarnings({"ConstantConditions"})
  public Flux<Record> updateStats(Flux<Record> recordFlux) {
    return recordFlux.doOnNext(
        record ->
            record.fields().stream()
                .filter(this::filterSizeFields)
                .forEach(
                    f -> {
                      Histogram histogram =
                          columnSizeStats.computeIfAbsent(
                              f.toString(), s -> new Histogram(new UniformReservoir()));
                      int size = (int) record.getFieldValue(f);
                      histogram.update(size);
                    }));
  }

  @SuppressWarnings({"ConstantConditions"})
  public boolean detectRecordWithLargeColumn(Record record) {
    return record.fields().stream()
        .filter(this::filterSizeFields)
        .mapToInt(f -> (int) record.getFieldValue(f))
        .anyMatch(size -> size > columnSizeThreshold);
  }

  public String generateReport() {
    StringBuilder report = new StringBuilder();
    long totalRows = totalRowCounter.longValue();
    long aboveThreshold = aboveThresholdCounter.longValue();
    double percentage = 0.0;
    if (totalRows > 0) {
      percentage = (double) aboveThreshold / totalRows;
    }
    report.append("=== Column size report ===\n");
    report.append("Total rows: ").append(totalRowCounter.longValue()).append("\n");
    report.append("Above threshold: ").append(aboveThresholdCounter.longValue()).append("\n");
    report.append("Percentage: ").append(String.format("%.2f", percentage * 100)).append("%\n\n");

    for (Map.Entry<String, Histogram> entry : columnSizeStats.entrySet()) {
      report.append("--- ").append(entry.getKey()).append(" ---\n");
      Snapshot snapshot = entry.getValue().getSnapshot();
      report.append("Min: ").append(snapshot.getMin()).append(" bytes\n");
      report.append("Mean: ").append(snapshot.getMean()).append(" bytes\n");
      report.append("Max: ").append(snapshot.getMax()).append(" bytes\n");
      report.append("Median: ").append(snapshot.getMedian()).append(" bytes\n");
      report.append("75%tile: ").append(snapshot.get75thPercentile()).append(" bytes\n");
      report.append("95%tile: ").append(snapshot.get95thPercentile()).append(" bytes\n");
      report.append("99%tile: ").append(snapshot.get99thPercentile()).append(" bytes\n");
      report.append('\n');
    }
    return report.toString();
  }

  private boolean filterSizeFields(Field f) {
    // Field name can start with "(double quote), since it is created as CQL identifier
    if (f.toString().startsWith("\"")) {
      return f.toString().substring(1).startsWith(COLUMN_PREFIX);
    }
    return f.toString().startsWith(COLUMN_PREFIX);
  }
}
