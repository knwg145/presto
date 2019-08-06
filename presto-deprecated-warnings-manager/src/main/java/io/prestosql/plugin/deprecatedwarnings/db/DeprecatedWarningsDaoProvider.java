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

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Inject;
import javax.inject.Provider;

import static java.util.Objects.requireNonNull;

public class DeprecatedWarningsDaoProvider
        implements Provider<DeprecatedWarningsDao>
{
    private final DeprecatedWarningsDao dao;

    @Inject
    public DeprecatedWarningsDaoProvider(DbDeprecatedWarningsManagerConfig config)
    {
        requireNonNull(config, "DbDeprecatedWarningsManagerConfig is null");
        String user = requireNonNull(config.getUsername(), "db username is null");
        String password = requireNonNull(config.getPassword(), "db password is null");
        String url = requireNonNull(config.getConfigDbUrl(), "db url is null");

        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUser(user);
        dataSource.setPassword(password);
        dataSource.setURL(url);

        this.dao = Jdbi.create(dataSource)
                .installPlugin(new SqlObjectPlugin())
                .onDemand(DeprecatedWarningsDao.class);
    }

    @Override
    public DeprecatedWarningsDao get()
    {
        return dao;
    }
}
