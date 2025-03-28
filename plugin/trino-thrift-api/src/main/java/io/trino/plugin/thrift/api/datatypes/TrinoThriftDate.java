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
package io.trino.plugin.thrift.api.datatypes;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;
import io.trino.plugin.thrift.api.TrinoThriftBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static io.trino.plugin.thrift.api.TrinoThriftBlock.dateData;
import static io.trino.plugin.thrift.api.datatypes.TrinoThriftTypeUtils.fromIntBasedBlock;
import static io.trino.plugin.thrift.api.datatypes.TrinoThriftTypeUtils.fromIntBasedColumn;
import static io.trino.spi.type.DateType.DATE;

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Elements of {@code dates} array are date values for each row represented as the number
 * of days passed since 1970-01-01.
 * If row is null then value is ignored.
 */
@ThriftStruct
public final class TrinoThriftDate
        implements TrinoThriftColumnData
{
    private final boolean[] nulls;
    private final int[] dates;

    @ThriftConstructor
    public TrinoThriftDate(
            @ThriftField(name = "nulls") @Nullable boolean[] nulls,
            @ThriftField(name = "dates") @Nullable int[] dates)
    {
        checkArgument(sameSizeIfPresent(nulls, dates), "nulls and values must be of the same size");
        this.nulls = nulls;
        this.dates = dates;
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public boolean[] getNulls()
    {
        return nulls;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public int[] getDates()
    {
        return dates;
    }

    @Override
    public Block toBlock(Type desiredType)
    {
        checkArgument(DATE.equals(desiredType), "type doesn't match: %s", desiredType);
        int numberOfRecords = numberOfRecords();
        return new IntArrayBlock(
                numberOfRecords,
                Optional.ofNullable(nulls),
                dates == null ? new int[numberOfRecords] : dates);
    }

    @Override
    public int numberOfRecords()
    {
        if (nulls != null) {
            return nulls.length;
        }
        if (dates != null) {
            return dates.length;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TrinoThriftDate other = (TrinoThriftDate) obj;
        return Arrays.equals(this.nulls, other.nulls) &&
                Arrays.equals(this.dates, other.dates);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(Arrays.hashCode(nulls), Arrays.hashCode(dates));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRecords", numberOfRecords())
                .toString();
    }

    public static TrinoThriftBlock fromBlock(Block block)
    {
        return fromIntBasedBlock(block, DATE, (nulls, ints) -> dateData(new TrinoThriftDate(nulls, ints)));
    }

    public static TrinoThriftBlock fromRecordSetColumn(RecordSet recordSet, int columnIndex, int totalRecords)
    {
        return fromIntBasedColumn(recordSet, columnIndex, totalRecords, (nulls, ints) -> dateData(new TrinoThriftDate(nulls, ints)));
    }

    private static boolean sameSizeIfPresent(boolean[] nulls, int[] dates)
    {
        return nulls == null || dates == null || nulls.length == dates.length;
    }
}
