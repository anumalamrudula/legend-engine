package com.gs.alloy.lakehouse.ingest.api.model.compiler;

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.CompileContext;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.IRelationalCompilerExtension;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Database;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.DynaFunc;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.ElementWithJoins;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.JoinPointer;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.Literal;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.LiteralList;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.RelationalOperationElement;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.TableAliasColumn;

import java.util.List;
import java.util.function.BiFunction;

public class LakehouseRelationalDatabaseCompilerExtension implements IRelationalCompilerExtension
{
    @Override
    public MutableList<String> group()
    {
        return Lists.mutable.with("Store", "Relational");
    }

    // Hook 1: preprocess Database before core compilation
    @Override
    public List<BiFunction<Database, CompileContext, Database>> getExtraDatabasePreProcessors()
    {
        return Lists.mutable.with((db, context) ->
        {
            if (db.importedIngests == null || db.importedIngests.isEmpty())
            {
                return db;
            }
            IngestCompilerExtension.generateAccessorTablesForImportedIngests(db, /*dbType*/ "H2");
            rewriteOperations(db);
            return db;
        });
    }

    private void rewriteOperations(Database db)
    {
        ListIterate.forEach(db.joins, j -> j.operation = rewrite(j.operation, db));
        ListIterate.forEach(db.filters, f -> f.operation = rewrite(f.operation, db));
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
            if ("lakehouseIngestColumn".equals(fn.funcName) && fn.parameters.size() == 1 && fn.parameters.get(0) instanceof LiteralList)
            {
                LiteralList parts = (LiteralList) fn.parameters.get(0);
                if (!parts.values.isEmpty() && parts.values.get(0) instanceof Literal)
                {
                    String schemaName = "default";
                    String tableName = parts.values.size() > 1 ? String.valueOf(((Literal) parts.values.get(1)).value) : null;
                    return IngestCompilerExtension.rewriteIngestColumnToTableRef(null, db.getPath(), schemaName, tableName);
                }
            }
            fn.parameters = ListIterate.collect(fn.parameters, p -> rewrite(p, db));
            return fn;
        }
        else if (op instanceof ElementWithJoins)
        {
            ElementWithJoins ej = (ElementWithJoins) op;
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