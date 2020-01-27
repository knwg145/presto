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

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.security.Identity;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.SetSessionAuthorization;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.transaction.TransactionManager;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class SetSessionAuthorizationTask
        implements DataDefinitionTask<SetSessionAuthorization>
{
    @Override
    public String getName()
    {
        return "SET SESSION AUTHORIZATION";
    }

    @Override
    public ListenableFuture<?> execute(SetSessionAuthorization statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        Identity originalIdentity = session.getOriginalIdentity().orElse(null);
        checkState(originalIdentity != null, "originalIdentity must be set");
        if (statement.getType() == SetSessionAuthorization.Type.USERNAME && !statement.getUsername().isPresent()) {
            throw new IllegalArgumentException("Username must be present for username type USERNAME");
        }

        switch (statement.getType()) {
            case DEFAULT:
                stateMachine.addSetSessionAuthorizationUsername(originalIdentity.getUser());
                break;
            case USERNAME:
                String username;
                Expression usernameType = statement.getUsername().get();
                if (usernameType instanceof Identifier) {
                    username = ((Identifier) usernameType).getValue();
                }
                else if (usernameType instanceof StringLiteral) {
                    username = ((StringLiteral) usernameType).getValue();
                }
                else {
                    throw new IllegalArgumentException("Unsupported usernameType: " + usernameType);
                }

                accessControl.canImpersonateUser(originalIdentity, username);
                stateMachine.addSetSessionAuthorizationUsername(username);
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + statement.getType());
        }

        return immediateFuture(null);
    }
}
