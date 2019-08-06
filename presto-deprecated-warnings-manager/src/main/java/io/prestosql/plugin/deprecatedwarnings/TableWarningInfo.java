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

import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningMessage;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TableWarningInfo
        extends DeprecatedWarningInfo
{
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final Optional<String> table;

    public TableWarningInfo(
            Optional<String> catalog,
            Optional<String> schema,
            Optional<String> table,
            Optional<String> grid,
            Optional<String> warningMessage,
            Optional<String> jiraLink)
    {
        super(grid, warningMessage, jiraLink);
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
    }

    public Optional<DeprecatedWarningMessage> match(String catalogName, String schemaName, String tableName, String grid)
    {
        if (!catalog.isPresent() || !catalog.get().equals(catalogName)) {
            return Optional.empty();
        }

        if (!schema.isPresent() || !schema.get().equals(schemaName)) {
            return Optional.empty();
        }

        if (!table.isPresent() || !table.get().equals(tableName)) {
            return Optional.empty();
        }

        if (!getGridInfo().isPresent() || !getGridInfo().get().equals(grid)) {
            return Optional.empty();
        }
        return Optional.of(new DeprecatedWarningMessage(getWarningMessage(), getJiraLink()));
    }

    public static class Mapper
            implements RowMapper<TableWarningInfo>
    {
        @Override
        public TableWarningInfo map(ResultSet resultSet, StatementContext ctx) throws SQLException
        {
            return new TableWarningInfo(
                Optional.ofNullable(resultSet.getString("catalogName")),
                Optional.ofNullable(resultSet.getString("schemaName")),
                Optional.ofNullable(resultSet.getString("tableName")),
                Optional.ofNullable(resultSet.getString("grid")),
                Optional.ofNullable(resultSet.getString("warningMessage")),
                Optional.ofNullable(resultSet.getString("jiraLink")));
        }
    }
}
