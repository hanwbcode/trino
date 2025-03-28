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
package io.trino.plugin.hive.fs;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// some tests may invalidate the whole cache affecting therefore other concurrent tests
@Execution(SAME_THREAD)
public class TestCachingDirectoryLister
        extends BaseCachingDirectoryListerTest<CachingDirectoryLister>
{
    @Override
    protected CachingDirectoryLister createDirectoryLister()
    {
        return new CachingDirectoryLister(Duration.valueOf("5m"), DataSize.of(1, MEGABYTE), List.of("tpch.*"));
    }

    @Override
    protected boolean isCached(CachingDirectoryLister directoryLister, Location location)
    {
        return directoryLister.isCached(location);
    }
}
