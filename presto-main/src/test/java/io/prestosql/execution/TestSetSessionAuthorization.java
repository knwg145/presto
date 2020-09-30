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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.prestosql.client.ClientSession;
import io.prestosql.client.QueryError;
import io.prestosql.client.StatementClient;
import io.prestosql.plugin.base.security.FileBasedSystemAccessControl;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;
import io.prestosql.testing.assertions.Assert;
import okhttp3.OkHttpClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.client.StatementClientFactory.newStatementClient;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestSetSessionAuthorization
{
    private TestingPrestoServer server;
    private OkHttpClient httpClient;

    @BeforeClass
    private void setUp()
    {
        server = TestingPrestoServer.builder()
                .setLoadAccessControlManager(false)
                .build();
        server.getAccessControl().addSystemAccessControlFactory(new TestingSystemAccessControlFactory());
        server.getAccessControl().setSystemAccessControl("file-test", ImmutableMap.of());
        httpClient = new OkHttpClient();
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
            throws Exception
    {
        server.close();
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    public class TestingSystemAccessControlFactory
            implements SystemAccessControlFactory
    {
        @Override
        public String getName()
        {
            return "file-test";
        }

        @Override
        public SystemAccessControl create(Map<String, String> config)
        {
            return newFileBasedSystemAccessControl("set_session_authorization_permissions.json");
        }
    }

    private SystemAccessControl newFileBasedSystemAccessControl(String resourceName)
    {
        return newFileBasedSystemAccessControl(ImmutableMap.of("security.config-file", getResourcePath(resourceName)));
    }

    private SystemAccessControl newFileBasedSystemAccessControl(ImmutableMap<String, String> config)
    {
        return new FileBasedSystemAccessControl.Factory().create(config);
    }

    private String getResourcePath(String resourceName)
    {
        return this.getClass().getClassLoader().getResource(resourceName).getPath();
    }

    @Test
    public void testSetSessionAuthorizationToSelf()
    {
        StatementClient client;
        client = submitQuery("SET SESSION AUTHORIZATION user", null, "user", null);
        Assert.assertEquals(client.getAuthorizationUser().get(), "user");

        client = submitQuery("SET SESSION AUTHORIZATION headlessAccount1", client.getAuthorizationUser().orElse(null), "user", null);
        Assert.assertEquals(client.getAuthorizationUser().get(), "headlessAccount1");

        client = submitQuery("SET SESSION AUTHORIZATION user", client.getAuthorizationUser().orElse(null), "user", null);
        Assert.assertEquals(client.getAuthorizationUser().get(), "user");
    }

    @Test
    public void testValidSetSessionAuthorization()
    {
        StatementClient client;
        client = submitQuery("SET SESSION AUTHORIZATION headlessAccount2", null, "user2", null);
        Assert.assertEquals(client.getAuthorizationUser().get(), "headlessAccount2");
        client = submitQuery("SET SESSION AUTHORIZATION headlessAccount1", null, "user", null);
        Assert.assertEquals(client.getAuthorizationUser().get(), "headlessAccount1");
    }

    @Test
    public void testInvalidSetSessionAuthorization()
    {
        StatementClient client = submitQuery("SET SESSION AUTHORIZATION user2", null, "user", null);
        QueryError error = client.currentStatusInfo().getError();
        Assert.assertEquals(client.getAuthorizationUser(), Optional.empty());
        Assert.assertEquals(error.getMessage(), "Access Denied: User user cannot impersonate user user2");

        client = submitQuery("SET SESSION AUTHORIZATION headlessAccount2", client.getAuthorizationUser().orElse(null), "user", null);
        error = client.currentStatusInfo().getError();
        Assert.assertEquals(client.getAuthorizationUser(), Optional.empty());
        Assert.assertEquals(error.getMessage(), "Access Denied: User user cannot impersonate user headlessAccount2");

        client = submitQuery("SET SESSION AUTHORIZATION headlessAccount1", client.getAuthorizationUser().orElse(null), "user", null);
        Assert.assertEquals(client.getAuthorizationUser().get(), "headlessAccount1");
        client = submitQuery("SET SESSION AUTHORIZATION user2", client.getAuthorizationUser().get(), "user", null);
        error = client.currentStatusInfo().getError();
        Assert.assertEquals(client.getAuthorizationUser(), Optional.empty());
        Assert.assertEquals(error.getMessage(), "Access Denied: User user cannot impersonate user user2");

        client = submitQuery("START TRANSACTION", null, "user", null);
        String transactionId = client.getStartedTransactionId();
        client = submitQuery("SET SESSION AUTHORIZATION headlessAccount1", null, "user", transactionId);
        error = client.currentStatusInfo().getError();
        Assert.assertEquals(error.getMessage(), "Can't set authorization user in the middle of a transaction");
    }

    @Test
    public void testValidSessionAuthorizationExecution()
    {
        StatementClient client = submitQuery("SELECT 1", "headlessAccount1", "user", null);
        QueryError error = client.currentStatusInfo().getError();
        Assert.assertEquals(error, null);

        client = submitQuery("SELECT 1", "user", "user", null);
        error = client.currentStatusInfo().getError();
        Assert.assertEquals(error, null);
    }

    @Test
    public void testInvalidSessionAuthorizationExecution()
    {
        StatementClient client = submitQuery("SELECT 1", "user2", "user", null);
        QueryError error = client.currentStatusInfo().getError();
        Assert.assertEquals(error.getMessage(), "Access Denied: User user cannot impersonate user user2");

        client = submitQuery("SELECT 1", "user3", "user", null);
        error = client.currentStatusInfo().getError();
        Assert.assertEquals(error.getMessage(), "Access Denied: User user cannot impersonate user user3");
    }

    @Test
    public void testResetSessionAuthorization()
    {
        StatementClient client;
        client = submitQuery("RESET SESSION AUTHORIZATION", null, "user", null);
        Assert.assertEquals(client.getResetAuthorizationUser().get(), "user");
        client = submitQuery("SET SESSION AUTHORIZATION headlessAccount1", null, client.getResetAuthorizationUser().orElse(null), null);
        Assert.assertEquals(client.getAuthorizationUser().get(), "headlessAccount1");
        client = submitQuery("RESET SESSION AUTHORIZATION", client.getAuthorizationUser().orElse(null), "user", null);
        Assert.assertEquals(client.getResetAuthorizationUser().get(), "user");

        client = submitQuery("START TRANSACTION", null, "user", null);
        String transactionId = client.getStartedTransactionId();
        client = submitQuery("RESET SESSION AUTHORIZATION", null, "user", transactionId);
        QueryError error = client.currentStatusInfo().getError();
        Assert.assertEquals(error.getMessage(), "Can't reset authorization user in the middle of a transaction");
    }

    private StatementClient submitQuery(String query, String authorizationUser, String user, String transactionId)
    {
        ClientSession clientSession = ClientSession.builder()
                .withServer(server.getBaseUrl())
                .withUser(user)
                .withSource("source")
                .withTimeZone(ZoneId.of("America/Los_Angeles"))
                .withLocale(Locale.ENGLISH)
                .withTransactionId(transactionId)
                .withClientRequestTimeout(new Duration(2, MINUTES))
                .withAuthorizationUser(Optional.ofNullable(authorizationUser))
                .build();

        // start query
        StatementClient client = newStatementClient(httpClient, clientSession, query);

        // wait for query to be fully scheduled
        while (client.isRunning() && !client.currentStatusInfo().getStats().isScheduled()) {
            client.advance();
        }
        return client;
    }
}
