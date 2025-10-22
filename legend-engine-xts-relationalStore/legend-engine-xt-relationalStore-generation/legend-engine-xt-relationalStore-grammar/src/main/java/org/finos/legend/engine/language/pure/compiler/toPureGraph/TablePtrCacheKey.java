// Copyright 2025 Goldman Sachs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.engine.language.pure.compiler.toPureGraph;

import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.mapping.TablePtr;

import java.util.Objects;

final class TablePtrCacheKey
{
    private final String database;
    private final String schema;
    private final String table;

    private TablePtrCacheKey(String database, String schema, String table)
    {
        this.database = database;
        this.schema = schema;
        this.table = table;
    }

    static TablePtrCacheKey of(TablePtr ptr)
    {
        return new TablePtrCacheKey(ptr == null ? null : ptr.database, ptr == null ? null : ptr.schema, ptr == null ? null : ptr.table);
    }

    static TablePtrCacheKey of(String database, String schema, String table)
    {
        return new TablePtrCacheKey(database, schema, table);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof TablePtrCacheKey)) return false;
        TablePtrCacheKey that = (TablePtrCacheKey) o;
        return Objects.equals(database, that.database)
                && Objects.equals(schema, that.schema)
                && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(database, schema, table);
    }
}
