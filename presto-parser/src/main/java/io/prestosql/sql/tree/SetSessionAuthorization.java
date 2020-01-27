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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SetSessionAuthorization
        extends Statement
{
    public enum Type
    {
        DEFAULT, USERNAME
    }

    private final Type type;
    private final Optional<? extends Expression> username;

    public SetSessionAuthorization(Type type, Optional<? extends Expression> username)
    {
        this(Optional.empty(), type, username);
    }

    public SetSessionAuthorization(NodeLocation location, Type type, Optional<? extends Expression> username)
    {
        this(Optional.of(location), type, username);
    }

    private SetSessionAuthorization(Optional<NodeLocation> location, Type type, Optional<? extends Expression> username)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.username = requireNonNull(username, "username is null");
    }

    public Type getType()
    {
        return type;
    }

    public Optional<? extends Expression> getUsername()
    {
        return username;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetSessionAuthorization(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SetSessionAuthorization setSessionAuthorization = (SetSessionAuthorization) o;
        return type == setSessionAuthorization.type &&
                Objects.equals(username, setSessionAuthorization.username);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, username);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("userName", username)
                .toString();
    }
}
