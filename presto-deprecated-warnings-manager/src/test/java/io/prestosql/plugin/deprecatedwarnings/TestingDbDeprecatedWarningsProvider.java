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
package io.prestosql.plugin.deprecatedwarnings;

import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.deprecatedwarnings.db.DbDeprecatedWarningsProvider;
import io.prestosql.spi.connector.StandardWarningCode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class TestingDbDeprecatedWarningsProvider
        implements DbDeprecatedWarningsProvider
{
    private Supplier<Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>>> loadingFunction;
    private final Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>> deprecatedWarningsMatch = new ConcurrentHashMap<>();
    private final AtomicBoolean isDestroyed = new AtomicBoolean(false);

    @Override
    public void initialize(Supplier<Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>>> loadingFunction)
    {
        this.loadingFunction = loadingFunction;
    }

    @Override
    public void destroy()
    {
        isDestroyed.compareAndSet(false, true);
    }

    @Override
    public Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>> get()
    {
        if (isDestroyed.get()) {
            throw new RuntimeException("Provider already destroyed");
        }
        Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>> updatedDeprecatedWarnings = loadingFunction.get();
        for (StandardWarningCode warnings : updatedDeprecatedWarnings.keySet()) {
            deprecatedWarningsMatch.put(warnings, updatedDeprecatedWarnings.get(warnings));
        }
        return ImmutableMap.copyOf(deprecatedWarningsMatch);
    }
}
