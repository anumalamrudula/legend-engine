package com.gs.alloy.lakehouse.ingest.api.model.compiler;

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.CompileContext;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.extension.CompilerExtension;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.extension.Processor;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Database;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.DynaFunc;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.ElementWithJoins;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.JoinPointer;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.Literal;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.LiteralList;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.RelationalOperationElement;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.TableAliasColumn;

import java.util.Collections;

public class LakehouseRelationalDatabaseCompilerExtension implements CompilerExtension
{
    @Override
    public MutableList<String> group()
    {
        return Lists.mutable.with("Store", "Relational");
    }

    @Override
    public CompilerExtension build()
    {
        return new LakehouseRelationalDatabaseCompilerExtension();
    }

    @Override
    public Iterable<? extends Processor<?>> getExtraProcessors()
    {
        return Collections.singletonList(
            Processor.newProcessor(
                Database.class,
                (db, context) -> { preProcessDatabase(db, context); return null; },
                null,
                null,
                null
            )
        );
    }

    private void preProcessDatabase(Database db, CompileContext context)
    {
        if (db.importedIngests == null || db.importedIngests.isEmpty())
        {
            return;
        }

        // 1) Generate relational tables from imported ingests (lineage-preserving)
        // TODO: Wire in actual Pure function call. Stub in place to keep control flow.
        IngestCompilerExtension.generateAccessorTablesForImportedIngests(db, /*dbType*/ "H2");

        // 2) Rewrite operations so joins/filters refer to the generated tables in the current database
        rewriteOperations(db);
    }

    private void rewriteOperations(Database db)
    {
        // joins
        ListIterate.forEach(db.joins, j -> j.operation = rewrite(j.operation, db));
        // filters
        ListIterate.forEach(db.filters, f -> f.operation = rewrite(f.operation, db));
        // views, view groupBy and columns
        ListIterate.forEach(db.schemas, s -> ListIterate.forEach(s.views, v ->
        {
            if (v.filter != null) { v.filter.operation = rewrite(v.filter.operation, db); }
            if (!v.groupBy.isEmpty()) { v.groupBy = ListIterate.collect(v.groupBy, op -> rewrite(op, db)); }
            if (!v.columnMappings.isEmpty()) { v.columnMappings.forEach(cm -> cm.operation = rewrite(cm.operation, db)); }
        }));
    }

    private RelationalOperationElement rewrite(RelationalOperationElement op, Database db)
    {
        if (op == null)
        {
            return null;
        }
        if (op instanceof DynaFunc)
        {
            DynaFunc fn = (DynaFunc) op;
            // Neutral marker created by the grammar/walker for ingest-qualified paths
            if ("lakehouseIngestColumn".equals(fn.funcName) && fn.parameters.size() == 1 && fn.parameters.get(0) instanceof LiteralList)
            {
                LiteralList parts = (LiteralList) fn.parameters.get(0);
                if (!parts.values.isEmpty() && parts.values.get(0) instanceof Literal)
                {
                    String schemaName = "default";
                    String tableName = parts.values.size() > 1 ? String.valueOf(((Literal) parts.values.get(1)).value) : null;
                    // Transform into a table column pointing at the current database
                    return IngestCompilerExtension.rewriteIngestColumnToTableRef(null, db.getPath(), schemaName, tableName);
                }
            }
            // Recurse into parameters
            fn.parameters = ListIterate.collect(fn.parameters, p -> rewrite(p, db));
            return fn;
        }
        else if (op instanceof ElementWithJoins)
        {
            ElementWithJoins ej = (ElementWithJoins) op;
            // Rewrite join pointer DBs that reference ingests to the current database
            if (ej.joins != null && !ej.joins.isEmpty() && db.importedIngests != null)
            {
                for (JoinPointer jp : ej.joins)
                {
                    if (jp.db != null && db.importedIngests.stream().anyMatch(p -> jp.db.equals(p.path)))
                    {
                        jp.db = db.getPath();
                    }
                }
            }
            ej.relationalElement = rewrite(ej.relationalElement, db);
            return ej;
        }
        else if (op instanceof TableAliasColumn)
        {
            TableAliasColumn tac = (TableAliasColumn) op;
            if (tac.table != null && tac.table.getDb() != null && db.importedIngests != null)
            {
                String dbPtr = tac.table.getDb();
                if (db.importedIngests.stream().anyMatch(p -> dbPtr.equals(p.path)))
                {
                    tac.table.database = db.getPath();
                    tac.table.mainTableDb = db.getPath();
                }
            }
            return tac;
        }
        else if (op instanceof LiteralList)
        {
            LiteralList ll = (LiteralList) op;
            ll.values = ListIterate.collect(ll.values, v -> rewrite(v, db));
            return ll;
        }
        else
        {
            return op;
        }
    }
}