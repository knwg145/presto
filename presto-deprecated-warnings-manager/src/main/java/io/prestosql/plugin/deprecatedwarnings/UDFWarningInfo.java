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

public class UDFWarningInfo
        extends DeprecatedWarningInfo
{
    private final Optional<String> signatureInfo;
    private final Optional<String> argumentsInfo;
    private final Optional<String> returnTypeInfo;

    public UDFWarningInfo(
            Optional<String> signatureInfo,
            Optional<String> argumentsInfo,
            Optional<String> returnTypeInfo,
            Optional<String> grid,
            Optional<String> warningMessage,
            Optional<String> jiraLink)
    {
        super(grid, warningMessage, jiraLink);
        this.signatureInfo = requireNonNull(signatureInfo, "signatureInfo is null");
        this.argumentsInfo = requireNonNull(argumentsInfo, "argumentsInfo is null");
        this.returnTypeInfo = requireNonNull(returnTypeInfo, "returnTypeInfo is null");
    }

    public Optional<DeprecatedWarningMessage> match(String signature, String arguments, String returnType, String grid)
    {
        if (!signatureInfo.isPresent() || !signature.equals(signatureInfo.get())) {
            return Optional.empty();
        }

        if (!argumentsInfo.isPresent() || !arguments.equals(argumentsInfo.get())) {
            return Optional.empty();
        }

        if (!returnTypeInfo.isPresent() || !returnType.equals(returnTypeInfo.get())) {
            return Optional.empty();
        }

        if (!getGridInfo().isPresent() || !grid.equals(getGridInfo().get())) {
            return Optional.empty();
        }
        return Optional.of(new DeprecatedWarningMessage(getWarningMessage(), getJiraLink()));
    }

    public static class Mapper
            implements RowMapper<UDFWarningInfo>
    {
        @Override
        public UDFWarningInfo map(ResultSet resultSet, StatementContext ctx) throws SQLException
        {
            return new UDFWarningInfo(
                Optional.ofNullable(resultSet.getString("signatureName")),
                Optional.ofNullable(resultSet.getString("arguments")),
                Optional.ofNullable(resultSet.getString("returnType")),
                Optional.ofNullable(resultSet.getString("grid")),
                Optional.ofNullable(resultSet.getString("warningMessage")),
                Optional.ofNullable(resultSet.getString("jiraLink")));
        }
    }
}
