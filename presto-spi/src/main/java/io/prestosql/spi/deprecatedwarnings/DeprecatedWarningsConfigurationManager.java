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
package io.prestosql.spi.deprecatedwarnings;

import java.util.List;

public interface DeprecatedWarningsConfigurationManager
{
    List<DeprecatedWarningMessage> getTableDeprecatedInfo(String catalog, String schema, String tableName);

    List<DeprecatedWarningMessage> getUDFDeprecatedInfo(String udfName, String arguments, String returnType);

    List<DeprecatedWarningMessage> getSessionPropertyDeprecatedInfo(String sessionPropertyName, String value);

    List<DeprecatedWarningMessage> getViewDeprecatedInfo(String viewName);
}
