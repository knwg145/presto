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
        USER
    }

    private final Type type;
    private final Optional<? extends Expression> user;

    public SetSessionAuthorization(Type type, Optional<? extends Expression> user)
    {
        this(Optional.empty(), type, user);
    }

    public SetSessionAuthorization(NodeLocation location, Type type, Optional<? extends Expression> user)
    {
        this(Optional.of(location), type, user);
    }

    private SetSessionAuthorization(Optional<NodeLocation> location, Type type, Optional<? extends Expression> user)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.user = requireNonNull(user, "user is null");
    }

    public Type getType()
    {
        return type;
    }

    public Optional<? extends Expression> getUser()
    {
        return user;
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
                Objects.equals(user, setSessionAuthorization.user);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, user);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("user", user)
                .toString();
    }
}
