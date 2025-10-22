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

import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Relational compile-time state bound to a {@link PureModel} instance.
 * This keeps relational-specific caches out of core {@code PureModel}.
 */
public final class RelationalCompileState
{
    private static final Map<PureModel, RelationalCompileState> BY_MODEL = new WeakHashMap<>();

    public static RelationalCompileState of(PureModel pureModel)
    {
        synchronized (BY_MODEL)
        {
            return BY_MODEL.computeIfAbsent(pureModel, pm -> new RelationalCompileState());
        }
    }

    private final ConcurrentMap<TablePtr, TablePtr> tableMappings = new ConcurrentHashMap<>();

    private RelationalCompileState()
    {
    }

    public void putTableMapping(TablePtr from, TablePtr to)
    {
        if (from != null && to != null)
        {
            this.tableMappings.put(from, to);
        }
    }

    public TablePtr resolveTable(TablePtr from)
    {
        return (from == null) ? null : this.tableMappings.get(from);
    }
}
