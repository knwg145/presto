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

public class SessionPropertyWarningInfo
        extends DeprecatedWarningInfo
{
    private final Optional<String> sessionProperty;
    private final Optional<String> badValue;

    public SessionPropertyWarningInfo(
            Optional<String> sessionProperty,
            Optional<String> badValue,
            Optional<String> grid,
            Optional<String> warningMessage,
            Optional<String> jiraLink)
    {
        super(grid, warningMessage, jiraLink);
        this.sessionProperty = requireNonNull(sessionProperty, "sessionProperty is null");
        this.badValue = requireNonNull(badValue, "badValue is null");
    }

    public Optional<DeprecatedWarningMessage> match(String sessionPropertyName, String sessionPropertyValue, String grid)
    {
        if (!sessionProperty.isPresent() || !sessionPropertyName.equals(sessionProperty.get())) {
            return Optional.empty();
        }

        if (!badValue.isPresent() || !sessionPropertyValue.equals(badValue.get())) {
            return Optional.empty();
        }

        if (!getGridInfo().isPresent() || !grid.equals(getGridInfo().get())) {
            return Optional.empty();
        }

        return Optional.of(new DeprecatedWarningMessage(getWarningMessage(), getJiraLink()));
    }

    public static class Mapper
            implements RowMapper<SessionPropertyWarningInfo>
    {
        @Override
        public SessionPropertyWarningInfo map(ResultSet resultSet, StatementContext ctx) throws SQLException
        {
            return new SessionPropertyWarningInfo(
                Optional.ofNullable(resultSet.getString("sessionPropertyName")),
                Optional.ofNullable(resultSet.getString("badValue")),
                Optional.ofNullable(resultSet.getString("grid")),
                Optional.ofNullable(resultSet.getString("warningMessage")),
                Optional.ofNullable(resultSet.getString("jiraLink")));
        }
    }
}
