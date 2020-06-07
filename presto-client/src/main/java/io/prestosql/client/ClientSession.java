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
package io.prestosql.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;

import java.net.URI;
import java.nio.charset.CharsetEncoder;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;

public class ClientSession
{
    private final URI server;
    private final String user;
    private final Optional<String> authorizationUser;
    private final String source;
    private final Optional<String> traceToken;
    private final Set<String> clientTags;
    private final String clientInfo;
    private final String catalog;
    private final String schema;
    private final String path;
    private final ZoneId timeZone;
    private final boolean useSessionTimeZone;
    private final Locale locale;
    private final Map<String, String> resourceEstimates;
    private final Map<String, String> properties;
    private final Map<String, String> preparedStatements;
    private final Map<String, ClientSelectedRole> roles;
    private final Map<String, String> extraCredentials;
    private final String transactionId;
    private final Duration clientRequestTimeout;

    public static Builder builder(ClientSession clientSession)
    {
        return new Builder(clientSession);
    }

    public static ClientSession stripTransactionId(ClientSession session)
    {
        return ClientSession.builder(session)
                .withoutTransactionId()
                .build();
    }

    public ClientSession(
            URI server,
            String user,
            Optional<String> authorizationUser,
            String source,
            Optional<String> traceToken,
            Set<String> clientTags,
            String clientInfo,
            String catalog,
            String schema,
            String path,
            ZoneId timeZone,
            boolean useSessionTimeZone,
            Locale locale,
            Map<String, String> resourceEstimates,
            Map<String, String> properties,
            Map<String, String> preparedStatements,
            Map<String, ClientSelectedRole> roles,
            Map<String, String> extraCredentials,
            String transactionId,
            Duration clientRequestTimeout)
    {
        this.server = requireNonNull(server, "server is null");
        this.user = user;
        this.authorizationUser = requireNonNull(authorizationUser, "authorizationUser is null");
        this.source = source;
        this.traceToken = requireNonNull(traceToken, "traceToken is null");
        this.clientTags = ImmutableSet.copyOf(requireNonNull(clientTags, "clientTags is null"));
        this.clientInfo = clientInfo;
        this.catalog = catalog;
        this.schema = schema;
        this.path = path;
        this.locale = locale;
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.useSessionTimeZone = useSessionTimeZone;
        this.transactionId = transactionId;
        this.resourceEstimates = ImmutableMap.copyOf(requireNonNull(resourceEstimates, "resourceEstimates is null"));
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        this.preparedStatements = ImmutableMap.copyOf(requireNonNull(preparedStatements, "preparedStatements is null"));
        this.roles = ImmutableMap.copyOf(requireNonNull(roles, "roles is null"));
        this.extraCredentials = ImmutableMap.copyOf(requireNonNull(extraCredentials, "extraCredentials is null"));
        this.clientRequestTimeout = clientRequestTimeout;

        for (String clientTag : clientTags) {
            checkArgument(!clientTag.contains(","), "client tag cannot contain ','");
        }

        // verify that resource estimates are valid
        CharsetEncoder charsetEncoder = US_ASCII.newEncoder();
        for (Entry<String, String> entry : resourceEstimates.entrySet()) {
            checkArgument(!entry.getKey().isEmpty(), "Resource name is empty");
            checkArgument(entry.getKey().indexOf('=') < 0, "Resource name must not contain '=': %s", entry.getKey());
            checkArgument(charsetEncoder.canEncode(entry.getKey()), "Resource name is not US_ASCII: %s", entry.getKey());
        }

        // verify the properties are valid
        for (Entry<String, String> entry : properties.entrySet()) {
            checkArgument(!entry.getKey().isEmpty(), "Session property name is empty");
            checkArgument(entry.getKey().indexOf('=') < 0, "Session property name must not contain '=': %s", entry.getKey());
            checkArgument(charsetEncoder.canEncode(entry.getKey()), "Session property name is not US_ASCII: %s", entry.getKey());
            checkArgument(charsetEncoder.canEncode(entry.getValue()), "Session property value is not US_ASCII: %s", entry.getValue());
        }

        // verify the extra credentials are valid
        for (Entry<String, String> entry : extraCredentials.entrySet()) {
            checkArgument(!entry.getKey().isEmpty(), "Credential name is empty");
            checkArgument(entry.getKey().indexOf('=') < 0, "Credential name must not contain '=': %s", entry.getKey());
            checkArgument(charsetEncoder.canEncode(entry.getKey()), "Credential name is not US_ASCII: %s", entry.getKey());
            checkArgument(charsetEncoder.canEncode(entry.getValue()), "Credential value is not US_ASCII: %s", entry.getValue());
        }
    }

    public URI getServer()
    {
        return server;
    }

    public String getUser()
    {
        return user;
    }

    public Optional<String> getAuthorizationUser()
    {
        return authorizationUser;
    }

    public String getSource()
    {
        return source;
    }

    public Optional<String> getTraceToken()
    {
        return traceToken;
    }

    public Set<String> getClientTags()
    {
        return clientTags;
    }

    public String getClientInfo()
    {
        return clientInfo;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getPath()
    {
        return path;
    }

    public ZoneId getTimeZone()
    {
        return timeZone;
    }

    // TODO remove the fallback mechanism for JDBC temporal columns
    @Deprecated
    public boolean useSessionTimeZone()
    {
        return useSessionTimeZone;
    }

    public Locale getLocale()
    {
        return locale;
    }

    public Map<String, String> getResourceEstimates()
    {
        return resourceEstimates;
    }

    public Map<String, String> getProperties()
    {
        return properties;
    }

    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    /**
     * Returns the map of catalog name -> selected role
     */
    public Map<String, ClientSelectedRole> getRoles()
    {
        return roles;
    }

    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    public String getTransactionId()
    {
        return transactionId;
    }

    public boolean isDebug()
    {
        return false;
    }

    public Duration getClientRequestTimeout()
    {
        return clientRequestTimeout;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("server", server)
                .add("user", user)
                .add("authorizationUser", authorizationUser.orElse(null))
                .add("clientTags", clientTags)
                .add("clientInfo", clientInfo)
                .add("catalog", catalog)
                .add("schema", schema)
                .add("path", path)
                .add("traceToken", traceToken.orElse(null))
                .add("timeZone", timeZone)
                .add("useSessionTimeZone", useSessionTimeZone)
                .add("locale", locale)
                .add("properties", properties)
                .add("transactionId", transactionId)
                .omitNullValues()
                .toString();
    }

    public static ClientSession.Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private URI server;
        private String user;
        private Optional<String> authorizationUser = Optional.empty();
        private String source;
        private Optional<String> traceToken = Optional.empty();
        private Set<String> clientTags = ImmutableSet.of();
        private String clientInfo;
        private String catalog;
        private String schema;
        private String path;
        private ZoneId timeZone;
        private boolean useSessionTimeZone;
        private Locale locale;
        private Map<String, String> resourceEstimates = ImmutableMap.of();
        private Map<String, String> properties = ImmutableMap.of();
        private Map<String, String> preparedStatements = ImmutableMap.of();
        private Map<String, ClientSelectedRole> roles = ImmutableMap.of();
        private Map<String, String> credentials = ImmutableMap.of();
        private String transactionId;
        private Duration clientRequestTimeout;

        private Builder() {}

        private Builder(ClientSession clientSession)
        {
            requireNonNull(clientSession, "clientSession is null");
            server = clientSession.getServer();
            user = clientSession.getUser();
            authorizationUser = clientSession.getAuthorizationUser();
            source = clientSession.getSource();
            traceToken = clientSession.getTraceToken();
            clientTags = clientSession.getClientTags();
            clientInfo = clientSession.getClientInfo();
            catalog = clientSession.getCatalog();
            schema = clientSession.getSchema();
            path = clientSession.getPath();
            timeZone = clientSession.getTimeZone();
            useSessionTimeZone = clientSession.useSessionTimeZone();
            locale = clientSession.getLocale();
            resourceEstimates = clientSession.getResourceEstimates();
            properties = clientSession.getProperties();
            preparedStatements = clientSession.getPreparedStatements();
            roles = clientSession.getRoles();
            credentials = clientSession.getExtraCredentials();
            transactionId = clientSession.getTransactionId();
            clientRequestTimeout = clientSession.getClientRequestTimeout();
        }

        public Builder withServer(URI server)
        {
            this.server = requireNonNull(server, "server is null");
            return this;
        }

        public Builder withUser(String user)
        {
            this.user = user;
            return this;
        }

        public Builder withAuthorizationUser(Optional<String> authorizationUser)
        {
            this.authorizationUser = requireNonNull(authorizationUser, "authorizationUser is null");
            return this;
        }

        public Builder withSource(String source)
        {
            this.source = source;
            return this;
        }

        public Builder withTraceToken(Optional<String> traceToken)
        {
            this.traceToken = requireNonNull(traceToken, "traceToken is null");
            return this;
        }

        public Builder withClientTags(Set<String> clientTags)
        {
            this.clientTags = requireNonNull(clientTags, "clientTags is null");
            return this;
        }

        public Builder withClientInfo(String clientInfo)
        {
            this.clientInfo = clientInfo;
            return this;
        }

        public Builder withCatalog(String catalog)
        {
            this.catalog = catalog;
            return this;
        }

        public Builder withSchema(String schema)
        {
            this.schema = schema;
            return this;
        }

        public Builder withPath(String path)
        {
            this.path = path;
            return this;
        }

        public Builder withTimeZone(ZoneId timeZone)
        {
            this.timeZone = requireNonNull(timeZone, "timeZone is null");
            return this;
        }

        public Builder withLocale(Locale locale)
        {
            this.locale = locale;
            return this;
        }

        public Builder withResourceEstimates(Map<String, String> resourceEstimates)
        {
            this.resourceEstimates = requireNonNull(resourceEstimates, "resourceEstimates is null");
            return this;
        }

        public Builder withProperties(Map<String, String> properties)
        {
            this.properties = requireNonNull(properties, "properties is null");
            return this;
        }

        public Builder withRoles(Map<String, ClientSelectedRole> roles)
        {
            this.roles = roles;
            return this;
        }

        public Builder withCredentials(Map<String, String> credentials)
        {
            this.credentials = requireNonNull(credentials, "extraCredentials is null");
            return this;
        }

        public Builder withPreparedStatements(Map<String, String> preparedStatements)
        {
            this.preparedStatements = requireNonNull(preparedStatements, "preparedStatements is null");
            return this;
        }

        public Builder withTransactionId(String transactionId)
        {
            this.transactionId = transactionId;
            return this;
        }

        public Builder withoutTransactionId()
        {
            this.transactionId = null;
            return this;
        }

        // TODO remove the fallback mechanism for JDBC temporal columns
        @Deprecated
        public Builder withUseSessionTimeZone(boolean useSessionTimeZone)
        {
            this.useSessionTimeZone = useSessionTimeZone;
            return this;
        }

        public Builder withClientRequestTimeout(Duration clientRequestTimeout)
        {
            this.clientRequestTimeout = clientRequestTimeout;
            return this;
        }

        public ClientSession build()
        {
            return new ClientSession(
                    server,
                    user,
                    authorizationUser,
                    source,
                    traceToken,
                    clientTags,
                    clientInfo,
                    catalog,
                    schema,
                    path,
                    timeZone,
                    useSessionTimeZone,
                    locale,
                    resourceEstimates,
                    properties,
                    preparedStatements,
                    roles,
                    credentials,
                    transactionId,
                    clientRequestTimeout);
        }
    }
}
