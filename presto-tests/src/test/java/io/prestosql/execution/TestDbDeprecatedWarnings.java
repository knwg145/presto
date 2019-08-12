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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.prestosql.Session;
import io.prestosql.execution.warnings.InternalDeprecatedWarningsManager;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.metadata.Signature;
import io.prestosql.plugin.deprecatedwarnings.H2DeprecatedWarningsDao;
import io.prestosql.plugin.deprecatedwarnings.H2DeprecatedWarningsDaoProvider;
import io.prestosql.plugin.deprecatedwarnings.TestingDbDeprecatedWarningsProvider;
import io.prestosql.plugin.deprecatedwarnings.db.DbDeprecatedWarningsManager;
import io.prestosql.plugin.deprecatedwarnings.db.DbDeprecatedWarningsManagerConfig;
import io.prestosql.plugin.deprecatedwarnings.db.DbDeprecatedWarningsProvider;
import io.prestosql.plugin.deprecatedwarnings.db.DeprecatedWarningsDao;
import io.prestosql.plugin.deprecatedwarnings.db.ForGrid;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningsConfigurationManager;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningsConfigurationManagerContext;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningsConfigurationManagerFactory;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.deprecatedwarnings.DeprecatedWarningContext;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.spi.connector.StandardWarningCode.SESSION_DEPRECATED_WARNINGS;
import static io.prestosql.spi.connector.StandardWarningCode.TABLE_DEPRECATED_WARNINGS;
import static io.prestosql.spi.connector.StandardWarningCode.UDF_DEPRECATED_WARNINGS;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class TestDbDeprecatedWarnings
{
    private TestingPrestoServer testingPrestoServer;
    private InternalDeprecatedWarningsManager internalDeprecatedWarningsManager;
    private Session session;
    private H2DeprecatedWarningsDao dao;
    private Analysis analysis;
    private String grid;

    @BeforeClass
    public void setup()
            throws Exception
    {
        this.testingPrestoServer = new TestingPrestoServer();
        this.internalDeprecatedWarningsManager = this.testingPrestoServer.getInternalDeprecatedWarningsManager();
        this.analysis = new Analysis(null, new ArrayList<>(), false);
        this.grid = internalDeprecatedWarningsManager.getConfigurationManagerContext().getEnvironment();
        testingPrestoServer.installPlugin(new TestDeprecatedWarningsConfigurationManagerPlugin());
        Map<String, String> configs = ImmutableMap.of("deprecated-warnings-manager.db.url",
                "jdbc:h2:mem:test" + System.nanoTime());

        this.session = Session.builder(new SessionPropertyManager())
                .setQueryId(new QueryId("test_query_id"))
                .setIdentity(new Identity("testUser", Optional.empty()))
                .setSystemProperty("query_priority", "1")
                .setCatalogSessionProperty("testCatalog", "explicit_set", "explicit_set")
                .setCatalog("hive")
                .setSchema("test")
                .build();

        setH2SessionPropertyConfigurationManager(configs);
        setupAnalysis(analysis);
        dao = getH2Dao(configs);

        dao.dropWarningsTable();
        dao.dropTableWarningsTable();
        dao.dropSessionPropertyWarningsTable();
        dao.dropUDFWarningsTable();

        dao.createWarningsTable();
        dao.createTableWarningsTable();
        dao.createSessionPropertyWarningsTable();
        dao.createUDFWarningsTable();

        dao.insertWarnings("testMessage", "jiraLinkTest");
        dao.insertWarnings("testMessage2", "jiraLinkTest2");
        dao.insertTableWarnings("hive", "test", "test", grid, 1);
        dao.insertUDFWarnings("count", "", "bigint", grid, 1);
        dao.insertSessionPropertyWarnings("query_priority", "1", grid, 1);
    }

    private void setupAnalysis(Analysis analysis)
    {
        Identity identity = new Identity("user", Optional.empty());
        AccessControl accessControl = new AllowAllAccessControl();
        QualifiedObjectName tableName = new QualifiedObjectName("hive", "test", "test");
        analysis.addEmptyColumnReferencesForTable(accessControl, identity, tableName);

        Map<NodeRef<FunctionCall>, Signature> functionSignature = new HashMap<>();
        FunctionCall functionCall = new FunctionCall(QualifiedName.of("count"), new ArrayList());
        Signature signature = new Signature("count", FunctionKind.SCALAR, new ArrayList(),
                new ArrayList(), new TypeSignature("bigint", new ArrayList()), new ArrayList(), true);
        functionSignature.put(NodeRef.of(functionCall), signature);
        analysis.addFunctionSignatures(functionSignature);

        analysis.registerView(tableName);
    }

    private DeprecatedWarningContext createDeprecatedWarningContext(Analysis analysis, Session session)
    {
        return new DeprecatedWarningContext(
                analysis
                    .getTableColumnReferences()
                    .values()
                    .stream()
                    .map(qualifiedObjectName -> qualifiedObjectName.keySet())
                    .flatMap(setOfQualifedObjectNames -> setOfQualifedObjectNames.stream())
                    .collect(ImmutableList.toImmutableList()),
                ImmutableList.copyOf(analysis.getFunctionSignature().values()),
                session.getSystemProperties(),
                analysis.getViews());
    }

    @Test
    public void testCombinedDynamicWarnings()
    {
        List<PrestoWarning> warnings = this.internalDeprecatedWarningsManager.getDeprecatedWarnings(createDeprecatedWarningContext(analysis, session));
        List<PrestoWarning> expectedWarnings = ImmutableList.of(
                new PrestoWarning(TABLE_DEPRECATED_WARNINGS,
                        "View hive.test.test has the following warning: testMessage : jiraLinkTest"),
                new PrestoWarning(UDF_DEPRECATED_WARNINGS,
                        "UDF: count():bigint has the following warning: testMessage : jiraLinkTest"),
                new PrestoWarning(SESSION_DEPRECATED_WARNINGS,
                        "Session Property: query_priority has the following warning: testMessage : jiraLinkTest"));
        assertWarning(warnings, expectedWarnings);

        warnings = this.internalDeprecatedWarningsManager.getDeprecatedWarnings(createDeprecatedWarningContext(new Analysis(null, new ArrayList<>(), false), session));
        assertFalse(warnings.stream().anyMatch(t -> t.getWarningCode().getCode() == 8));
        assertFalse(warnings.stream().anyMatch(t -> t.getWarningCode().getCode() == 9));
        assertWarning(warnings, ImmutableList.of(new PrestoWarning(SESSION_DEPRECATED_WARNINGS,
                "Session Property: query_priority has the following warning: testMessage : jiraLinkTest")));
    }

    private void assertWarning(List<PrestoWarning> warningList, List<PrestoWarning> expectedWarnings)
    {
        assertTrue(warningList.size() == expectedWarnings.size());
        for (PrestoWarning warning : warningList) {
            assertTrue(expectedWarnings.contains(warning));
        }
    }

    private void setH2SessionPropertyConfigurationManager(Map<String, String> configs)
    {
        internalDeprecatedWarningsManager.setConfigurationManager("h2-test", configs);
    }

    private H2DeprecatedWarningsDao getH2Dao(Map<String, String> configs)
    {
        DbDeprecatedWarningsManagerConfig managerConfig = new DbDeprecatedWarningsManagerConfig();
        managerConfig.setConfigDbUrl(configs.get("deprecated-warnings-manager.db.url"));

        H2DeprecatedWarningsDaoProvider provider = new H2DeprecatedWarningsDaoProvider(managerConfig);
        return provider.get();
    }

    static class TestDeprecatedWarningsConfigurationManagerPlugin
            implements Plugin
    {
        @Override
        public Iterable<DeprecatedWarningsConfigurationManagerFactory> getDeprecatedWarningsConfigurationManagerFactories()
        {
            return ImmutableList.of(new TestDbDeprecatedWarningsManagerFactory());
        }
    }

    static class TestDbDeprecatedWarningsManagerModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(TestingDbDeprecatedWarningsProvider.class).in(Scopes.SINGLETON);
            configBinder(binder).bindConfig(DbDeprecatedWarningsManagerConfig.class);
            binder.bind(DbDeprecatedWarningsManager.class).in(Scopes.SINGLETON);
            binder.bind(DeprecatedWarningsDao.class).toProvider(H2DeprecatedWarningsDaoProvider.class).in(Scopes.SINGLETON);
            binder.bind(DbDeprecatedWarningsProvider.class).to(TestingDbDeprecatedWarningsProvider.class).in(Scopes.SINGLETON);
            newExporter(binder).export(DbDeprecatedWarningsManager.class).withGeneratedName();
        }
    }

    static class TestDbDeprecatedWarningsManagerFactory
            implements DeprecatedWarningsConfigurationManagerFactory
    {
        @Override
        public String getName()
        {
            return "h2-test";
        }

        @Override
        public DeprecatedWarningsConfigurationManager create(Map<String, String> config, DeprecatedWarningsConfigurationManagerContext context)
        {
            try {
                Bootstrap app = new Bootstrap(
                        new JsonModule(),
                        new TestDbDeprecatedWarningsManagerModule(),
                        binder -> binder.bind(String.class).annotatedWith(ForGrid.class).toInstance(context.getEnvironment()));

                Injector injector = app
                        .strictConfig()
                        .doNotInitializeLogging()
                        .setRequiredConfigurationProperties(config)
                        .initialize();
                return injector.getInstance(DbDeprecatedWarningsManager.class);
            }
            catch (Exception e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
    }
}
