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
package io.prestosql.plugin.deprecatedwarnings.db;

import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.deprecatedwarnings.DeprecatedWarningInfo;
import io.prestosql.spi.connector.StandardWarningCode;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * Periodically schedules the loadingFunction during initialization
 * and returns the most recently and correctly loaded warnings with
 * every get() call.
 */
public class RefreshingDbWarningsProvider
        implements DbDeprecatedWarningsProvider
{
    private final Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>> deprecatedWarningsMatch = new ConcurrentHashMap();
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("DbDeprecatedWarningsManager"));
    private final AtomicBoolean started = new AtomicBoolean();

    private final long refreshPeriodMillis;

    @Inject
    public RefreshingDbWarningsProvider(DbDeprecatedWarningsManagerConfig config)
    {
        requireNonNull(config, "config is null");
        this.refreshPeriodMillis = config.getWarningsRefreshPeriod().toMillis();
    }

    @Override
    public void initialize(Supplier<Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>>> loadingFunction)
    {
        requireNonNull(loadingFunction, "loadingFunction is null");

        if (started.compareAndSet(false, true)) {
            executor.scheduleWithFixedDelay(() -> {
                Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>> updatedDeprecatedWarnings = loadingFunction.get();
                for (StandardWarningCode warning : updatedDeprecatedWarnings.keySet()) {
                    deprecatedWarningsMatch.put(warning, updatedDeprecatedWarnings.get(warning));
                }
            }, 0, refreshPeriodMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void destroy()
    {
        executor.shutdownNow();
    }

    @Override
    public Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>> get()
    {
        return ImmutableMap.copyOf(deprecatedWarningsMatch);
    }
}
