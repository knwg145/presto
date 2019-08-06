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

import io.prestosql.plugin.deprecatedwarnings.DeprecatedWarningInfo;
import io.prestosql.spi.connector.StandardWarningCode;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public interface DbDeprecatedWarningsProvider
        extends Supplier<Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>>>
{
    void initialize(Supplier<Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>>> loader);

    void destroy();
}
