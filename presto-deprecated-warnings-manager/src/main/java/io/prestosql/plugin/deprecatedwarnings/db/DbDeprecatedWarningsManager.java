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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.prestosql.plugin.deprecatedwarnings.DeprecatedWarningInfo;
import io.prestosql.plugin.deprecatedwarnings.SessionPropertyWarningInfo;
import io.prestosql.plugin.deprecatedwarnings.TableWarningInfo;
import io.prestosql.plugin.deprecatedwarnings.UDFWarningInfo;
import io.prestosql.plugin.deprecatedwarnings.ViewWarningInfo;
import io.prestosql.spi.connector.StandardWarningCode;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningMessage;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningsConfigurationManager;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.spi.connector.StandardWarningCode.SESSION_DEPRECATED_WARNINGS;
import static io.prestosql.spi.connector.StandardWarningCode.TABLE_DEPRECATED_WARNINGS;
import static io.prestosql.spi.connector.StandardWarningCode.UDF_DEPRECATED_WARNINGS;
import static io.prestosql.spi.connector.StandardWarningCode.VIEW_DEPRECATED_WARNINGS;
import static java.util.Objects.requireNonNull;

public class DbDeprecatedWarningsManager
        implements DeprecatedWarningsConfigurationManager
{
    private final DeprecatedWarningsDao dao;
    private final DbDeprecatedWarningsProvider warningsProvider;
    private static final Logger log = Logger.get(DbDeprecatedWarningsManager.class);
    private final CounterStat tableWarningsDbLoadFailuresCounter = new CounterStat();
    private final CounterStat udfWarningsDbLoadFailuresCounter = new CounterStat();
    private final CounterStat sessionPropertyWarningsDbLoadFailuresCounter = new CounterStat();
    private final CounterStat viewWarningsDbLoadFailuresCounter = new CounterStat();
    private final String grid;

    @Inject
    public DbDeprecatedWarningsManager(
            DeprecatedWarningsDao dao,
            DbDeprecatedWarningsProvider warningProvider,
            @ForGrid String grid)
    {
        this.dao = requireNonNull(dao, "DeprecatedWarningsDao is null");
        this.warningsProvider = requireNonNull(warningProvider, "warningsProvider is null");
        this.grid = requireNonNull(grid, "grid is null");
    }

    @PreDestroy
    public void destroy()
    {
        warningsProvider.destroy();
    }

    @PostConstruct
    public void start()
    {
        dao.createWarningsTable();
        dao.createTableWarningsTable();
        dao.createUDFWarningsTable();
        dao.createSessionPropertyWarningsTable();
        dao.createViewWarningsTable();
        warningsProvider.initialize(this::loadWarnings);
    }

    @Override
    public List<DeprecatedWarningMessage> getTableDeprecatedInfo(String catalog, String schema, String tableName)
    {
        Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>> warningsInfo = warningsProvider.get();
        if (!warningsInfo.containsKey(TABLE_DEPRECATED_WARNINGS)) {
            log.error("Does not contain tableWarnings");
            return ImmutableList.of();
        }
        ImmutableList.Builder<DeprecatedWarningMessage> allMessages = new ImmutableList.Builder<>();
        for (DeprecatedWarningInfo deprecatedWarningDbInfo : warningsInfo.get(TABLE_DEPRECATED_WARNINGS)) {
            TableWarningInfo tableWarningDbInfo = (TableWarningInfo) deprecatedWarningDbInfo;
            Optional<DeprecatedWarningMessage> deprecatedDbInfo = tableWarningDbInfo.match(catalog, schema, tableName, grid);
            if (deprecatedDbInfo.isPresent()) {
                allMessages.add(deprecatedDbInfo.get());
            }
        }
        return allMessages.build();
    }

    @Override
    public List<DeprecatedWarningMessage> getUDFDeprecatedInfo(String udfName, String arguments, String returnType)
    {
        Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>> warningsInfo = warningsProvider.get();
        if (!warningsInfo.containsKey(UDF_DEPRECATED_WARNINGS)) {
            log.error("Does not contain udfWarnings");
            return ImmutableList.of();
        }
        ImmutableList.Builder<DeprecatedWarningMessage> allMessages = new ImmutableList.Builder<>();
        for (DeprecatedWarningInfo deprecatedWarningDbInfo : warningsInfo.get(UDF_DEPRECATED_WARNINGS)) {
            UDFWarningInfo udfWarningDbInfo = (UDFWarningInfo) deprecatedWarningDbInfo;
            Optional<DeprecatedWarningMessage> deprecatedDbInfo = udfWarningDbInfo.match(udfName, arguments, returnType, grid);
            if (deprecatedDbInfo.isPresent()) {
                allMessages.add(deprecatedDbInfo.get());
            }
        }
        return allMessages.build();
    }

    @Override
    public List<DeprecatedWarningMessage> getSessionPropertyDeprecatedInfo(String sessionPropertyName, String value)
    {
        Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>> warningsInfo = warningsProvider.get();
        if (!warningsInfo.containsKey(SESSION_DEPRECATED_WARNINGS)) {
            log.error("Does not contain sessionPropertyWarnings");
            return ImmutableList.of();
        }
        ImmutableList.Builder<DeprecatedWarningMessage> allMessages = new ImmutableList.Builder<>();
        for (DeprecatedWarningInfo deprecatedWarningDbInfo : warningsInfo.get(SESSION_DEPRECATED_WARNINGS)) {
            SessionPropertyWarningInfo sessionPropetyWarningDbInfo = (SessionPropertyWarningInfo) deprecatedWarningDbInfo;
            Optional<DeprecatedWarningMessage> deprecatedDbInfo = sessionPropetyWarningDbInfo.match(sessionPropertyName, value, grid);
            if (deprecatedDbInfo.isPresent()) {
                allMessages.add(deprecatedDbInfo.get());
            }
        }
        return allMessages.build();
    }

    @Override
    public List<DeprecatedWarningMessage> getViewDeprecatedInfo(String viewName)
    {
        Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>> warningsInfo = warningsProvider.get();
        if (!warningsInfo.containsKey(VIEW_DEPRECATED_WARNINGS)) {
            log.error("Does not contain viewWarnings");
            return ImmutableList.of();
        }
        ImmutableList.Builder<DeprecatedWarningMessage> allMessages = new ImmutableList.Builder<>();
        for (DeprecatedWarningInfo deprecatedWarningDbInfo : warningsInfo.get(VIEW_DEPRECATED_WARNINGS)) {
            ViewWarningInfo viewWarningDbInfo = (ViewWarningInfo) deprecatedWarningDbInfo;
            Optional<DeprecatedWarningMessage> deprecatedDbInfo = viewWarningDbInfo.match(viewName, grid);
            if (deprecatedDbInfo.isPresent()) {
                allMessages.add(deprecatedDbInfo.get());
            }
        }
        return allMessages.build();
    }

    public Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>> loadWarnings()
    {
        Map<StandardWarningCode, List<? extends DeprecatedWarningInfo>> deprecatedWarningsMapping = new HashMap<>();
        try {
            deprecatedWarningsMapping.put(TABLE_DEPRECATED_WARNINGS, dao.getTableWarnings());
        }
        catch (Exception e) {
            tableWarningsDbLoadFailuresCounter.update(1);
            log.error("Error loading tableWarnings");
        }
        try {
            deprecatedWarningsMapping.put(UDF_DEPRECATED_WARNINGS, dao.getUDFWarnings());
        }
        catch (Exception e) {
            udfWarningsDbLoadFailuresCounter.update(1);
            log.error("Error loading udfWarnings");
        }
        try {
            deprecatedWarningsMapping.put(SESSION_DEPRECATED_WARNINGS, dao.getSessionPropertyWarnings());
        }
        catch (Exception e) {
            sessionPropertyWarningsDbLoadFailuresCounter.update(1);
            log.error("Error loading sessionPropertyWarnings");
        }
        try {
            deprecatedWarningsMapping.put(VIEW_DEPRECATED_WARNINGS, dao.getViewWarnings());
        }
        catch (Exception e) {
            viewWarningsDbLoadFailuresCounter.update(1);
            log.error("Error loading viewWarnings");
        }
        return deprecatedWarningsMapping;
    }

    @Managed
    @Nested
    public CounterStat getTableWarningsDbLoadFailuresCounter()
    {
        return tableWarningsDbLoadFailuresCounter;
    }

    @Managed
    @Nested
    public CounterStat getUdfWarningsDbLoadFailuresCounter()
    {
        return udfWarningsDbLoadFailuresCounter;
    }

    @Managed
    @Nested
    public CounterStat getSessionPropertyWarningsDbLoadFailuresCounter()
    {
        return sessionPropertyWarningsDbLoadFailuresCounter;
    }

    @Managed
    @Nested
    public CounterStat getViewWarningsDbLoadFailuresCounter()
    {
        return viewWarningsDbLoadFailuresCounter;
    }
}
