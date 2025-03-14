/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.tpch.TpchTable.NATION;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergSplitSource
        extends AbstractTestQueryFramework
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new IcebergSessionProperties(
                    new IcebergConfig(),
                    new OrcReaderConfig(),
                    new OrcWriterConfig(),
                    new ParquetReaderConfig(),
                    new ParquetWriterConfig())
                    .getSessionProperties())
            .build();

    private File metastoreDir;
    private TrinoFileSystemFactory fileSystemFactory;
    private TrinoCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File tempDir = Files.createTempDirectory("test_iceberg_split_source").toFile();
        this.metastoreDir = new File(tempDir, "iceberg_data");
        HiveMetastore metastore = createTestingFileHiveMetastore(metastoreDir);

        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setInitialTables(NATION)
                .setMetastoreDirectory(metastoreDir)
                .build();

        this.fileSystemFactory = getFileSystemFactory(queryRunner);
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastore, 1000);
        this.catalog = new TrinoHiveCatalog(
                new CatalogName("hive"),
                cachingHiveMetastore,
                new TrinoViewHiveMetastore(cachingHiveMetastore, false, "trino-version", "test"),
                fileSystemFactory,
                new TestingTypeManager(),
                new FileMetastoreTableOperationsProvider(fileSystemFactory),
                false,
                false,
                false,
                new IcebergConfig().isHideMaterializedViewStorageTable());

        return queryRunner;
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.getParentFile().toPath(), ALLOW_INSECURE);
    }

    @Test
    @Timeout(30)
    public void testIncompleteDynamicFilterTimeout()
            throws Exception
    {
        long startMillis = System.currentTimeMillis();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", "nation");
        Table nationTable = catalog.loadTable(SESSION, schemaTableName);
        IcebergTableHandle tableHandle = new IcebergTableHandle(
                CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                TableType.DATA,
                Optional.empty(),
                SchemaParser.toJson(nationTable.schema()),
                Optional.of(PartitionSpecParser.toJson(nationTable.spec())),
                1,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                nationTable.location(),
                nationTable.properties(),
                false,
                Optional.empty(),
                ImmutableSet.of(),
                Optional.of(false));

        try (IcebergSplitSource splitSource = new IcebergSplitSource(
                fileSystemFactory,
                SESSION,
                tableHandle,
                nationTable.newScan(),
                Optional.empty(),
                new DynamicFilter()
                {
                    @Override
                    public Set<ColumnHandle> getColumnsCovered()
                    {
                        return ImmutableSet.of();
                    }

                    @Override
                    public CompletableFuture<?> isBlocked()
                    {
                        return CompletableFuture.runAsync(() -> {
                            try {
                                TimeUnit.HOURS.sleep(1);
                            }
                            catch (InterruptedException e) {
                                throw new IllegalStateException(e);
                            }
                        });
                    }

                    @Override
                    public boolean isComplete()
                    {
                        return false;
                    }

                    @Override
                    public boolean isAwaitable()
                    {
                        return true;
                    }

                    @Override
                    public TupleDomain<ColumnHandle> getCurrentPredicate()
                    {
                        return TupleDomain.all();
                    }
                },
                new Duration(2, SECONDS),
                alwaysTrue(),
                new TestingTypeManager(),
                false,
                new IcebergConfig().getMinimumAssignedSplitWeight())) {
            ImmutableList.Builder<IcebergSplit> splits = ImmutableList.builder();
            while (!splitSource.isFinished()) {
                splitSource.getNextBatch(100).get()
                        .getSplits()
                        .stream()
                        .map(IcebergSplit.class::cast)
                        .forEach(splits::add);
            }
            assertThat(splits.build().size()).isGreaterThan(0);
            assertThat(splitSource.isFinished()).isTrue();
            assertThat(System.currentTimeMillis() - startMillis)
                    .as("IcebergSplitSource failed to wait for dynamicFilteringWaitTimeout")
                    .isGreaterThanOrEqualTo(2000);
        }
    }

    @Test
    public void testBigintPartitionPruning()
    {
        IcebergColumnHandle bigintColumn = new IcebergColumnHandle(
                new ColumnIdentity(1, "name", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                Optional.empty());
        assertThat(IcebergSplitSource.partitionMatchesPredicate(
                ImmutableSet.of(bigintColumn),
                () -> ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1000L)),
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 100L))))).isFalse();
        assertThat(IcebergSplitSource.partitionMatchesPredicate(
                ImmutableSet.of(bigintColumn),
                () -> ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1000L)),
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1000L))))).isTrue();
        assertThat(IcebergSplitSource.partitionMatchesPredicate(
                ImmutableSet.of(bigintColumn),
                () -> ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1000L)),
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.asNull(BIGINT))))).isFalse();
    }

    @Test
    public void testBigintStatisticsPruning()
    {
        IcebergColumnHandle bigintColumn = new IcebergColumnHandle(
                new ColumnIdentity(1, "name", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                Optional.empty());
        Map<Integer, Type.PrimitiveType> primitiveTypes = ImmutableMap.of(1, Types.LongType.get());
        Map<Integer, ByteBuffer> lowerBound = ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), 1000L));
        Map<Integer, ByteBuffer> upperBound = ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), 2000L));

        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 0L))),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L))).isFalse();
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1000L))),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L))).isTrue();
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1500L))),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L))).isTrue();
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 2000L))),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L))).isTrue();
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 3000L))),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L))).isFalse();

        Domain outsideStatisticsRangeAllowNulls = Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, true, 100L, true)), true);
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, outsideStatisticsRangeAllowNulls)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L))).isFalse();
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, outsideStatisticsRangeAllowNulls)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 1L))).isTrue();

        Domain outsideStatisticsRangeNoNulls = Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, true, 100L, true)), false);
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, outsideStatisticsRangeNoNulls)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L))).isFalse();
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, outsideStatisticsRangeNoNulls)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 1L))).isFalse();

        Domain insideStatisticsRange = Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1001L, true, 1002L, true)), false);
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, insideStatisticsRange)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L))).isTrue();
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, insideStatisticsRange)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 1L))).isTrue();

        Domain overlappingStatisticsRange = Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 990L, true, 1010L, true)), false);
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, overlappingStatisticsRange)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L))).isTrue();
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, overlappingStatisticsRange)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 1L))).isTrue();
    }

    @Test
    public void testNullStatisticsMaps()
    {
        IcebergColumnHandle bigintColumn = new IcebergColumnHandle(
                new ColumnIdentity(1, "name", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                Optional.empty());
        Map<Integer, Type.PrimitiveType> primitiveTypes = ImmutableMap.of(1, Types.LongType.get());
        Map<Integer, ByteBuffer> lowerBound = ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), -1000L));
        Map<Integer, ByteBuffer> upperBound = ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), 2000L));
        TupleDomain<IcebergColumnHandle> domainOfZero = TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 0L)));

        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                domainOfZero,
                null,
                upperBound,
                ImmutableMap.of(1, 0L))).isTrue();
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                domainOfZero,
                ImmutableMap.of(),
                upperBound,
                ImmutableMap.of(1, 0L))).isTrue();

        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                domainOfZero,
                lowerBound,
                null,
                ImmutableMap.of(1, 0L))).isTrue();
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                domainOfZero,
                lowerBound,
                ImmutableMap.of(),
                ImmutableMap.of(1, 0L))).isTrue();

        TupleDomain<IcebergColumnHandle> onlyNull = TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, Domain.onlyNull(BIGINT)));
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                onlyNull,
                ImmutableMap.of(),
                ImmutableMap.of(),
                null)).isTrue();
        assertThat(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                onlyNull,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of())).isTrue();
    }
}
