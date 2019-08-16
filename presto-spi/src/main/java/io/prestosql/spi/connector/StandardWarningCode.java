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
package io.prestosql.spi.connector;

import io.prestosql.spi.WarningCode;
import io.prestosql.spi.WarningCodeSupplier;

public enum StandardWarningCode
        implements WarningCodeSupplier
{
    TOO_MANY_STAGES(0x0000_0001),
    REDUNDANT_ORDER_BY(0x0000_0002),
    ORC_SUGGESTION(0x0000_0100),
    TOO_MANY_PARTITIONS(0x0000_0101),
    UNSUPPORTED_COL_TYPES(0x0000_0102),
    OVER_THRESHOLD_VAL(0x0000_0103),
    STAGE_SKEW(0x0000_0104),
    BETTER_JOIN_ORDERING(0x0000_0105),
    TABLE_DEPRECATED_WARNINGS(0x0000_0106),
    UDF_DEPRECATED_WARNINGS(0x0000_0107),
    SESSION_DEPRECATED_WARNINGS(0x0000_0108),
    VIEW_DEPRECATED_WARNINGS(0x0000_0109)

    /**/;
    private final WarningCode warningCode;

    StandardWarningCode(int code)
    {
        warningCode = new WarningCode(code, name());
    }

    @Override
    public WarningCode toWarningCode()
    {
        return warningCode;
    }
}
