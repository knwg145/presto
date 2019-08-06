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

import io.prestosql.plugin.deprecatedwarnings.db.DbDeprecatedWarningsManagerConfig;
import io.prestosql.plugin.deprecatedwarnings.db.DeprecatedWarningsDao;
import org.h2.jdbcx.JdbcDataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Inject;
import javax.inject.Provider;

import static java.util.Objects.requireNonNull;

public class H2DeprecatedWarningsDaoProvider
        implements Provider<DeprecatedWarningsDao>
{
    private final H2DeprecatedWarningsDao dao;

    @Inject
    public H2DeprecatedWarningsDaoProvider(DbDeprecatedWarningsManagerConfig config)
    {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(requireNonNull(config.getConfigDbUrl(), "deprecated-warnings-manager.db.url is null"));

        this.dao = Jdbi.create(ds)
                .installPlugin(new SqlObjectPlugin())
                .open()
                .attach(H2DeprecatedWarningsDao.class);
    }

    @Override
    public H2DeprecatedWarningsDao get()
    {
        return dao;
    }
}
