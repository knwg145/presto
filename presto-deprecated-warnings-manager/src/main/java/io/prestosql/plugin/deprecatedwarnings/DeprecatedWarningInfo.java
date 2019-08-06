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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class DeprecatedWarningInfo
{
    private final Optional<String> gridInfo;
    private final Optional<String> warningMessage;
    private final Optional<String> jiraLink;

    protected DeprecatedWarningInfo(
            Optional<String> gridInfo,
            Optional<String> warningMessage,
            Optional<String> jiraLink)
    {
        this.gridInfo = requireNonNull(gridInfo, "gridInfo is null");
        this.warningMessage = requireNonNull(warningMessage, "warningMessage is null");
        this.jiraLink = requireNonNull(jiraLink, "jiraLink is null");
    }

    public Optional<String> getGridInfo()
    {
        return gridInfo;
    }

    public Optional<String> getWarningMessage()
    {
        return warningMessage;
    }

    public Optional<String> getJiraLink()
    {
        return jiraLink;
    }
}
