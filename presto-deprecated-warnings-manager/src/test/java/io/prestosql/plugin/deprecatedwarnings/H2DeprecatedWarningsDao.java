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

import io.prestosql.plugin.deprecatedwarnings.db.DeprecatedWarningsDao;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface H2DeprecatedWarningsDao
        extends DeprecatedWarningsDao
{
    @SqlUpdate("INSERT INTO warnings(warningMessage, jiraLink)\n" +
            "VALUES (\n" +
            "   :warningMessage,\n" +
            "   :jiraLink\n" +
            ")")
    void insertWarnings(@Bind("warningMessage") String warningMessage,
                    @Bind("jiraLink") String jiraLink);

    @SqlUpdate("INSERT INTO tableWarnings(catalogName, schemaName, tableName, grid, warningId)\n" +
            "VALUES (\n" +
            "   :catalogName,\n" +
            "   :schemaName,\n" +
            "   :tableName,\n" +
            "   :grid,\n" +
            "   :warningId\n" +
            ")")
    void insertTableWarnings(@Bind("catalogName") String catalogName,
                    @Bind("schemaName") String schemeName,
                    @Bind("tableName") String tableName,
                    @Bind("grid") String grid,
                    @Bind("warningId") long warningId);

    @SqlUpdate("INSERT INTO udfWarnings(signatureName, arguments, returnType, grid, warningId)\n" +
            "VALUES (\n" +
            "   :signatureName,\n" +
            "   :arguments,\n" +
            "   :returnType,\n" +
            "   :grid,\n" +
            "   :warningId\n" +
            ")")
    void insertUDFWarnings(@Bind("signatureName") String signatureName,
                    @Bind("arguments") String arguments,
                    @Bind("returnType") String returnType,
                    @Bind("grid") String grid,
                    @Bind("warningId") long warningId);

    @SqlUpdate("INSERT INTO sessionPropertyWarnings(sessionPropertyName, badValue, grid, warningId)\n" +
            "VALUES (\n" +
            "   :sessionPropertyName,\n" +
            "   :badValue,\n" +
            "   :grid,\n" +
            "   :warningId\n" +
            ")")
    void insertSessionPropertyWarnings(@Bind("sessionPropertyName") String sessionPropertyName,
                    @Bind("badValue") String badValue,
                    @Bind("grid") String grid,
                    @Bind("warningId") long warningId);

    @SqlUpdate("INSERT INTO viewWarnings(view, grid, warningId)\n" +
            "VALUES (\n" +
            "   :view,\n" +
            "   :grid,\n" +
            "   :warningId\n" +
            ")")
    void insertViewWarnings(@Bind("view") String view,
                    @Bind("grid") String grid,
                    @Bind("warningId") long warningId);
}
