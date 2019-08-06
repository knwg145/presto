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
package io.prestosql.execution.warnings;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.connector.StandardWarningCode;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningMessage;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningsConfigurationManager;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningsConfigurationManagerContext;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningsConfigurationManagerFactory;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.deprecatedwarnings.DeprecatedWarningContext;
import io.prestosql.sql.deprecatedwarnings.DeprecatedWarningsConfigurationManagerContextInstance;
import io.prestosql.sql.tree.Table;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.connector.StandardWarningCode.SESSION_DEPRECATED_WARNINGS;
import static io.prestosql.spi.connector.StandardWarningCode.TABLE_DEPRECATED_WARNINGS;
import static io.prestosql.spi.connector.StandardWarningCode.UDF_DEPRECATED_WARNINGS;
import static io.prestosql.spi.connector.StandardWarningCode.VIEW_DEPRECATED_WARNINGS;
import static io.prestosql.util.PropertiesUtil.loadProperties;
import static java.lang.String.format;

public class InternalDeprecatedWarningsManager
{
    private static final Logger log = Logger.get(InternalDeprecatedWarningsManager.class);
    private static final File DEPRECATED_WARNINGS_CONFIGURATION = new File("etc/deprecated-warnings-config.properties");
    private static final String DEPRECATED_WARNINGS_MANAGER_NAME = "deprecated-warnings-config.configuration-manager";

    private final Map<String, DeprecatedWarningsConfigurationManagerFactory> factories = new ConcurrentHashMap<>();
    private final AtomicReference<DeprecatedWarningsConfigurationManager> delegate = new AtomicReference<>();
    private final DeprecatedWarningsConfigurationManagerContext configurationManagerContext;
    private final String grid;

    @Inject
    public InternalDeprecatedWarningsManager(NodeInfo nodeInfo)
    {
        configurationManagerContext = new DeprecatedWarningsConfigurationManagerContextInstance(nodeInfo.getEnvironment());
        grid = configurationManagerContext.getEnvironment();
    }

    public List<PrestoWarning> getDeprecatedWarnings(DeprecatedWarningContext deprecatedWarningContext)
    {
        if (delegate.get() == null) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<PrestoWarning> allWarnings = new ImmutableList.Builder<>();

        Set<String> tableDeprecatedWarnings = generateTableDeprecatedWarning(
                deprecatedWarningContext.getTables(),
                deprecatedWarningContext.getCatalog(),
                deprecatedWarningContext.getSchema());
        if (!tableDeprecatedWarnings.isEmpty()) {
            allWarnings.add(generatePrestoWarning(
                    "The query may have issues because of the following table(s). ",
                    tableDeprecatedWarnings,
                    TABLE_DEPRECATED_WARNINGS));
        }

        Set<String> udfDeprecatedWarnings = generateUDFDeprecatedWarning(deprecatedWarningContext.getFunctionSignatures());
        if (!udfDeprecatedWarnings.isEmpty()) {
            allWarnings.add(generatePrestoWarning(
                    "The query may have issues because of the following UDF(s). ",
                    udfDeprecatedWarnings,
                    UDF_DEPRECATED_WARNINGS));
        }

        Set<String> sessionPropertyDeprecatedWarnings = generateSessionPropertyDeprecatedWarning(deprecatedWarningContext.getSystemProperties());
        if (!sessionPropertyDeprecatedWarnings.isEmpty()) {
            allWarnings.add(generatePrestoWarning(
                    "The query may have issues because of the following session property(s). ",
                    sessionPropertyDeprecatedWarnings,
                    SESSION_DEPRECATED_WARNINGS));
        }

        Set<String> viewDeprecatedWarnings = generateViewDeprecatedWarning(deprecatedWarningContext.getViews());
        if (!viewDeprecatedWarnings.isEmpty()) {
            allWarnings.add(generatePrestoWarning(
                    "The query may have issues because of the following view(s). ",
                    viewDeprecatedWarnings,
                    VIEW_DEPRECATED_WARNINGS));
        }
        return allWarnings.build();
    }

    private PrestoWarning generatePrestoWarning(String highLevelMessage, Set<String> warnings, StandardWarningCode standardWarningCode)
    {
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append(highLevelMessage);
        messageBuilder.append(String.join(",", warnings));
        return new PrestoWarning(standardWarningCode, messageBuilder.toString());
    }

    private Set<String> generateTableDeprecatedWarning(
            List<Table> tables,
            Optional<String> catalog,
            Optional<String> schema)
    {
        ImmutableSet.Builder<String> warnings = new ImmutableSet.Builder<>();
        for (Table table : tables) {
            List<String> tableParts = getTableParts(table, catalog, schema);
            String catalogName = tableParts.get(0);
            String schemaName = tableParts.get(1);
            String tableName = tableParts.get(2);

            List<DeprecatedWarningMessage> deprecatedWarningMessages = delegate.get().getTableDeprecatedInfo(catalogName, schemaName, tableName);
            for (DeprecatedWarningMessage deprecatedWarningMessage : deprecatedWarningMessages) {
                String warningMessage = deprecatedWarningMessage.getWarningMessage().get();
                String jiraLink = deprecatedWarningMessage.getJiraLink().get();
                warnings.add(format("%s.%s.%s has the following warning: %s : %s in grid %s",
                        catalogName, schemaName, tableName, warningMessage, jiraLink, grid));
            }
        }
        return warnings.build();
    }

    private List<String> getTableParts(Table table, Optional<String> catalog, Optional<String> schema)
    {
        String catalogName = "";
        String schemaName = "";
        String tableName = "";
        if (catalog.isPresent()) {
            catalogName = catalog.get();
        }
        if (schema.isPresent()) {
            schemaName = schema.get();
        }

        List<String> parts = table.getName().getParts();
        for (int i = parts.size() - 1; i >= 0; i--) {
            if (i == parts.size() - 1) {
                tableName = parts.get(i);
            }
            else if (i == parts.size() - 2) {
                schemaName = parts.get(i);
            }
            else if (i == parts.size() - 3) {
                catalogName = parts.get(i);
            }
        }
        return ImmutableList.of(catalogName, schemaName, tableName);
    }

    private Set<String> generateUDFDeprecatedWarning(List<Signature> functionSignatures)
    {
        ImmutableSet.Builder<String> warnings = new ImmutableSet.Builder<>();
        for (Signature signature : functionSignatures) {
            String functionName = signature.getName();
            String arguments = signature.getArgumentTypes().stream().map(TypeSignature::getBase).collect(Collectors.joining(","));
            String returnType = signature.getReturnType().getBase();

            List<DeprecatedWarningMessage> deprecatedWarningMessages = delegate.get().getUDFDeprecatedInfo(functionName, arguments, returnType);
            for (DeprecatedWarningMessage deprecatedWarningMessage : deprecatedWarningMessages) {
                String warningMessage = deprecatedWarningMessage.getWarningMessage().get();
                String jiraLink = deprecatedWarningMessage.getJiraLink().get();
                warnings.add(format("UDF: %s(%s):%s has the following warning: %s : %s in grid %s", functionName, arguments, returnType, warningMessage, jiraLink, grid));
            }
        }
        return warnings.build();
    }

    private Set<String> generateSessionPropertyDeprecatedWarning(Map<String, String> systemProperties)
    {
        ImmutableSet.Builder<String> warnings = new ImmutableSet.Builder<>();
        for (String systemProperty : systemProperties.keySet()) {
            List<DeprecatedWarningMessage> deprecatedWarningMessages =
                    delegate.get().getSessionPropertyDeprecatedInfo(systemProperty, systemProperties.get(systemProperty));
            for (DeprecatedWarningMessage deprecatedWarningMessage : deprecatedWarningMessages) {
                String warningMessage = deprecatedWarningMessage.getWarningMessage().get();
                String jiraLink = deprecatedWarningMessage.getJiraLink().get();
                warnings.add(format("Session Property: %s has the following warning: %s : %s in grid %s",
                        systemProperty, warningMessage, jiraLink, grid));
            }
        }
        return warnings.build();
    }

    private Set<String> generateViewDeprecatedWarning(Set<Table> views)
    {
        ImmutableSet.Builder<String> warnings = new ImmutableSet.Builder<>();
        for (Table table : views) {
            String tableName = table.getName().toString();
            List<DeprecatedWarningMessage> deprecatedWarningMessages = delegate.get().getViewDeprecatedInfo(tableName);
            for (DeprecatedWarningMessage deprecatedWarningMessage : deprecatedWarningMessages) {
                String warningMessage = deprecatedWarningMessage.getWarningMessage().get();
                String jiraLink = deprecatedWarningMessage.getJiraLink().get();
                warnings.add(format("View: %s has the following warning: %s : %s in grid %s",
                        tableName, warningMessage, jiraLink, grid));
            }
        }
        return warnings.build();
    }

    public void addConfigurationManagerFactory(
            DeprecatedWarningsConfigurationManagerFactory deprecatedWarningsConfigurationManagerFactory)
    {
        if (factories.putIfAbsent(deprecatedWarningsConfigurationManagerFactory.getName(),
                deprecatedWarningsConfigurationManagerFactory) != null) {
            throw new IllegalArgumentException(format("Deprecated warnings property configuration manager '%s' is already registered", deprecatedWarningsConfigurationManagerFactory
                .getName()));
        }
    }

    public void loadConfigurationManager()
            throws IOException
    {
        if (!DEPRECATED_WARNINGS_CONFIGURATION.exists()) {
            return;
        }

        Map<String, String> propertyMap = new HashMap<>(loadProperties(DEPRECATED_WARNINGS_CONFIGURATION));

        log.info("-- Loading deprecated warnings configuration manager --");

        String configurationManagerName = propertyMap.remove(DEPRECATED_WARNINGS_MANAGER_NAME);
        checkArgument(configurationManagerName != null, "Deprecated warnings configuration %s does not contain %s", DEPRECATED_WARNINGS_CONFIGURATION, DEPRECATED_WARNINGS_MANAGER_NAME);

        setConfigurationManager(configurationManagerName, propertyMap);

        log.info("-- Loaded deprecated warnings configuration manager %s --", configurationManagerName);
    }

    @VisibleForTesting
    public void setConfigurationManager(String name, Map<String, String> properties)
    {
        DeprecatedWarningsConfigurationManagerFactory factory = factories.get(name);
        checkState(factory != null, "Deprecated warnings configuration manager %s is not registered", name);

        DeprecatedWarningsConfigurationManager manager = factory.create(properties, configurationManagerContext);
        checkState(delegate.compareAndSet(null, manager), "deprecatedWarningsConfigurationManager is already set");
    }

    @VisibleForTesting
    public DeprecatedWarningsConfigurationManagerContext getConfigurationManagerContext()
    {
        return configurationManagerContext;
    }
}
