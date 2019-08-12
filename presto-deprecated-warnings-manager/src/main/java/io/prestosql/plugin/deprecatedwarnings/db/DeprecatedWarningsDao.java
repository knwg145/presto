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

import io.prestosql.plugin.deprecatedwarnings.SessionPropertyWarningInfo;
import io.prestosql.plugin.deprecatedwarnings.TableWarningInfo;
import io.prestosql.plugin.deprecatedwarnings.UDFWarningInfo;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;

import java.util.List;

public interface DeprecatedWarningsDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS warnings (\n" +
            "warningId BIGINT NOT NULL AUTO_INCREMENT,\n" +
            "warningMessage VARCHAR(512),\n" +
            "jiraLink VARCHAR(512),\n" +
            "PRIMARY KEY (warningId)\n" +
            ")\n")
    void createWarningsTable();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS tableWarnings (\n" +
            "tableWarningId BIGINT NOT NULL AUTO_INCREMENT,\n" +
            "catalogName VARCHAR(512),\n" +
            "schemaName VARCHAR(512),\n" +
            "tableName VARCHAR(512),\n" +
            "grid VARCHAR(512),\n" +
            "warningId BIGINT,\n" +
            "FOREIGN KEY(warningId) REFERENCES warnings(warningId),\n" +
            "PRIMARY KEY (tableWarningId)\n" +
            ")\n")
    void createTableWarningsTable();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS udfWarnings (\n" +
            "udfWarningId BIGINT NOT NULL AUTO_INCREMENT,\n" +
            "signatureName VARCHAR(512),\n" +
            "arguments VARCHAR(512),\n" +
            "returnType VARCHAR(512),\n" +
            "grid VARCHAR(512),\n" +
            "warningId BIGINT,\n" +
            "FOREIGN KEY(warningId) REFERENCES warnings(warningId),\n" +
            "PRIMARY KEY (udfWarningId)\n" +
            ")\n")
    void createUDFWarningsTable();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS sessionPropertyWarnings (\n" +
            "sessionPropertyWarningId BIGINT NOT NULL AUTO_INCREMENT,\n" +
            "sessionPropertyName VARCHAR(512),\n" +
            "badValue VARCHAR(512),\n" +
            "grid VARCHAR(512),\n" +
            "warningId BIGINT,\n" +
            "FOREIGN KEY(warningId) REFERENCES warnings(warningId),\n" +
            "PRIMARY KEY (sessionPropertyWarningId)\n" +
            ")\n")
    void createSessionPropertyWarningsTable();

    @SqlQuery(
            "SELECT " +
            "a.catalogName," +
            "a.schemaName, " +
            "a.tableName, " +
            "a.grid, " +
            "b.warningMessage, " +
            "b.jiraLink " +
            "FROM " +
            "tableWarnings a " +
            "JOIN " +
            "warnings b " +
            "on a.warningId = b.warningId; ")
    @UseRowMapper(TableWarningInfo.Mapper.class)
    List<TableWarningInfo> getTableWarnings();

    @SqlQuery(
            "SELECT " +
            "a.signatureName," +
            "a.arguments, " +
            "a.returnType, " +
            "a.grid, " +
            "b.warningMessage, " +
            "b.jiraLink " +
            "FROM " +
            "udfWarnings a " +
            "JOIN " +
            "warnings b " +
            "on a.warningId = b.warningId; ")
    @UseRowMapper(UDFWarningInfo.Mapper.class)
    List<UDFWarningInfo> getUDFWarnings();

    @SqlQuery(
            "SELECT " +
            "a.sessionPropertyName," +
            "a.badValue," +
            "a.grid, " +
            "b.warningMessage, " +
            "b.jiraLink " +
            "FROM " +
            "sessionPropertyWarnings a " +
            "JOIN " +
            "warnings b " +
            "on a.warningId = b.warningId; ")
    @UseRowMapper(SessionPropertyWarningInfo.Mapper.class)
    List<SessionPropertyWarningInfo> getSessionPropertyWarnings();

    @SqlUpdate("DROP TABLE IF EXISTS warnings")
    void dropWarningsTable();

    @SqlUpdate("DROP TABLE IF EXISTS tableWarnings")
    void dropTableWarningsTable();

    @SqlUpdate("DROP TABLE IF EXISTS udfWarnings")
    void dropUDFWarningsTable();

    @SqlUpdate("DROP TABLE IF EXISTS sessionPropertyWarnings")
    void dropSessionPropertyWarningsTable();
}
