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

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import static java.util.concurrent.TimeUnit.SECONDS;

public class DbDeprecatedWarningsManagerConfig
{
    private String configUrl = "jdbc:mysql://localhost:3306";
    private String username = "user";
    private String password = "password";
    private Duration warningsRefreshPeriod = new Duration(5, SECONDS);

    public String getConfigDbUrl()
    {
        return configUrl;
    }

    @Config("deprecated-warnings-manager.db.url")
    public DbDeprecatedWarningsManagerConfig setConfigDbUrl(String configUrl)
    {
        this.configUrl = configUrl;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    @Config("deprecated-warnings-manager.db.username")
    public DbDeprecatedWarningsManagerConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("deprecated-warnings-manager.db.password")
    public DbDeprecatedWarningsManagerConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @Config("deprecated-warnings-manager.db.refresh-period")
    public DbDeprecatedWarningsManagerConfig setWarningsRefreshPeriod(Duration warningsRefreshPeriod)
    {
        this.warningsRefreshPeriod = warningsRefreshPeriod;
        return this;
    }

    @MinDuration("1ms")
    public Duration getWarningsRefreshPeriod()
    {
        return warningsRefreshPeriod;
    }
}
