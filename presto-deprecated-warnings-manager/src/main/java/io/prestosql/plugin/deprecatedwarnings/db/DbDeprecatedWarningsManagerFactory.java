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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningsConfigurationManager;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningsConfigurationManagerContext;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningsConfigurationManagerFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;

public class DbDeprecatedWarningsManagerFactory
        implements DeprecatedWarningsConfigurationManagerFactory
{
    @Override
    public String getName()
    {
        return "db";
    }

    @Override
    public DeprecatedWarningsConfigurationManager create(Map<String, String> config, DeprecatedWarningsConfigurationManagerContext context)
    {
        try {
            Bootstrap app = new Bootstrap(
                    new MBeanModule(),
                    new MBeanServerModule(),
                    new DbDeprecatedWarningsManagerModule(),
                    binder -> binder.bind(String.class).annotatedWith(ForGrid.class).toInstance(context.getEnvironment()));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(DbDeprecatedWarningsManager.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
