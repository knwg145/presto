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

import io.prestosql.plugin.deprecatedwarnings.db.DbDeprecatedWarningsManager;
import io.prestosql.plugin.deprecatedwarnings.db.DbDeprecatedWarningsManagerConfig;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningMessage;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDbDeprecatedWarningsManager
{
    private DbDeprecatedWarningsManagerConfig config;
    private H2DeprecatedWarningsDaoProvider daoProvider;
    private H2DeprecatedWarningsDao dao;
    private DbDeprecatedWarningsManager manager;

    @BeforeClass
    public void setup()
    {
        config = getH2Config();
        daoProvider = new H2DeprecatedWarningsDaoProvider(config);
        dao = daoProvider.get();
        manager = new DbDeprecatedWarningsManager(dao, new TestingDbDeprecatedWarningsProvider(), "1");
        manager.start();
    }

    @BeforeMethod
    public void setupTest()
    {
        dao.dropWarningsTable();
        dao.dropTableWarningsTable();
        dao.dropSessionPropertyWarningsTable();
        dao.dropUDFWarningsTable();
        dao.dropViewWarningsTable();

        dao.createWarningsTable();
        dao.createTableWarningsTable();
        dao.createSessionPropertyWarningsTable();
        dao.createUDFWarningsTable();
        dao.createViewWarningsTable();

        dao.insertWarnings("testMessage", "jiraLinkTest");
        dao.insertWarnings("testMessage2", "jiraLinkTest2");
    }

    static DbDeprecatedWarningsManagerConfig getH2Config()
    {
        return new DbDeprecatedWarningsManagerConfig().setConfigDbUrl("jdbc:h2:mem:test_dynamic_warnings" + System.nanoTime() + ";MODE=MySQL");
    }

    @Test
    public void testTableDynamicWarnings()
    {
        List<DeprecatedWarningMessage> deprecatedWarningMessages = manager.getTableDeprecatedInfo("hive", "testSchema", "test");
        assertTrue(deprecatedWarningMessages.isEmpty());

        dao.insertTableWarnings("hive", "testSchema", "test", "1", 1);
        try {
            Thread.sleep(5000);
        }
        catch (Exception e) {
        }

        deprecatedWarningMessages = manager.getTableDeprecatedInfo("hive", "testSchema", "test");
        assertMessage(deprecatedWarningMessages.get(0), "testMessage", "jiraLinkTest");
    }

    @Test
    public void testUDFDynamicWarnings()
    {
        List<DeprecatedWarningMessage> deprecatedWarningMessages = manager.getUDFDeprecatedInfo("count", "", "bigint");
        assertTrue(deprecatedWarningMessages.isEmpty());

        dao.insertUDFWarnings("count", "", "bigint", "1", 1);
        try {
            Thread.sleep(5000);
        }
        catch (Exception e) {
        }

        deprecatedWarningMessages = manager.getUDFDeprecatedInfo("count", "", "bigint");
        assertMessage(deprecatedWarningMessages.get(0), "testMessage", "jiraLinkTest");
    }

    @Test
    public void testSessionPropertyDynamicWarnings()
    {
        List<DeprecatedWarningMessage> deprecatedWarningMessages = manager.getSessionPropertyDeprecatedInfo("query_priority", "1");
        assertTrue(deprecatedWarningMessages.isEmpty());

        dao.insertSessionPropertyWarnings("query_priority", "1", "1", 1);
        try {
            Thread.sleep(5000);
        }
        catch (Exception e) {
        }

        deprecatedWarningMessages = manager.getSessionPropertyDeprecatedInfo("query_priority", "1");
        assertMessage(deprecatedWarningMessages.get(0), "testMessage", "jiraLinkTest");
    }

    @Test
    public void testViewDynamicWarnings()
    {
        List<DeprecatedWarningMessage> deprecatedWarningMessages = manager.getViewDeprecatedInfo("test");
        assertTrue(deprecatedWarningMessages.isEmpty());

        dao.insertViewWarnings("test", "1", 1);
        try {
            Thread.sleep(5000);
        }
        catch (Exception e) {
        }

        deprecatedWarningMessages = manager.getViewDeprecatedInfo("test");
        assertMessage(deprecatedWarningMessages.get(0), "testMessage", "jiraLinkTest");
    }

    @Test
    public void testReload()
    {
        List<DeprecatedWarningMessage> deprecatedWarningMessages = manager.getViewDeprecatedInfo("test");
        assertTrue(deprecatedWarningMessages.isEmpty());

        dao.insertViewWarnings("test", "1", 1);
        dao.dropViewWarningsTable();
        try {
            Thread.sleep(5000);
        }
        catch (Exception e) {
        }

        deprecatedWarningMessages = manager.getViewDeprecatedInfo("test");
        assertTrue(deprecatedWarningMessages.isEmpty());

        dao.createViewWarningsTable();
        dao.insertViewWarnings("test", "1", 1);

        try {
            Thread.sleep(5000);
        }
        catch (Exception e) {
        }

        deprecatedWarningMessages = manager.getViewDeprecatedInfo("test");
        assertMessage(deprecatedWarningMessages.get(0), "testMessage", "jiraLinkTest");
    }

    @Test
    public void testMultipleMatches()
    {
        dao.insertViewWarnings("test", "1", 1);
        dao.insertViewWarnings("test", "1", 2);
        List<DeprecatedWarningMessage> deprecatedWarningMessages = manager.getViewDeprecatedInfo("test");
        assertTrue(deprecatedWarningMessages.size() == 2);
        assertMessage(deprecatedWarningMessages.get(0), "testMessage", "jiraLinkTest");
        assertMessage(deprecatedWarningMessages.get(1), "testMessage2", "jiraLinkTest2");
    }

    @Test
    public void testNullInDb()
    {
        dao.insertViewWarnings("test", null, 1);
        List<DeprecatedWarningMessage> deprecatedWarningMessages = manager.getViewDeprecatedInfo("test");
        assertTrue(deprecatedWarningMessages.size() == 0);

        dao.insertSessionPropertyWarnings("query_priority", "1", null, 1);
        deprecatedWarningMessages = manager.getSessionPropertyDeprecatedInfo("query_priority", "1");
        assertTrue(deprecatedWarningMessages.size() == 0);
    }

    private void assertMessage(DeprecatedWarningMessage deprecatedWarningMessage, String warningMessage, String jiraLink)
    {
        assertTrue(deprecatedWarningMessage.getWarningMessage().get().equals(warningMessage));
        assertTrue(deprecatedWarningMessage.getJiraLink().get().equals(jiraLink));
    }
}
