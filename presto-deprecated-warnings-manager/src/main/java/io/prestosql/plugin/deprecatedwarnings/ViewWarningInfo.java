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

public class ViewWarningInfo
        extends DeprecatedWarningInfo
{
    private final Optional<String> view;

    public ViewWarningInfo(
            Optional<String> view,
            Optional<String> grid,
            Optional<String> warningMessage,
            Optional<String> jiraLink)
    {
        super(grid, warningMessage, jiraLink);
        this.view = requireNonNull(view, "view is null");
    }

    public Optional<DeprecatedWarningMessage> match(String viewName, String grid)
    {
        if (!view.isPresent() || !view.get().equals(viewName)) {
            return Optional.empty();
        }
        if (!getGridInfo().isPresent() || !getGridInfo().get().equals(grid)) {
            return Optional.empty();
        }
        return Optional.of(new DeprecatedWarningMessage(getWarningMessage(), getJiraLink()));
    }

    public static class Mapper
            implements RowMapper<ViewWarningInfo>
    {
        @Override
        public ViewWarningInfo map(ResultSet resultSet, StatementContext ctx) throws SQLException
        {
            return new ViewWarningInfo(
                Optional.ofNullable(resultSet.getString("view")),
                Optional.ofNullable(resultSet.getString("grid")),
                Optional.ofNullable(resultSet.getString("warningMessage")),
                Optional.ofNullable(resultSet.getString("jiraLink")));
        }
    }
}
