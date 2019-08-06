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
import io.airlift.units.Duration;
import io.prestosql.plugin.deprecatedwarnings.db.DbDeprecatedWarningsManagerConfig;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestDbDeprecatedWarningsManagerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DbDeprecatedWarningsManagerConfig.class)
                .setConfigDbUrl("jdbc:mysql://localhost:3306")
                .setUsername("user")
                .setPassword("password")
                .setWarningsRefreshPeriod(new Duration(5, TimeUnit.SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("deprecated-warnings-manager.db.url", "foo")
                .put("deprecated-warnings-manager.db.username", "bar")
                .put("deprecated-warnings-manager.db.password", "pass")
                .put("deprecated-warnings-manager.db.refresh-period", "50s")
                .build();

        DbDeprecatedWarningsManagerConfig expected = new DbDeprecatedWarningsManagerConfig()
                .setConfigDbUrl("foo")
                .setUsername("bar")
                .setPassword("pass")
                .setWarningsRefreshPeriod(new Duration(50, TimeUnit.SECONDS));

        assertFullMapping(properties, expected);
    }
}
