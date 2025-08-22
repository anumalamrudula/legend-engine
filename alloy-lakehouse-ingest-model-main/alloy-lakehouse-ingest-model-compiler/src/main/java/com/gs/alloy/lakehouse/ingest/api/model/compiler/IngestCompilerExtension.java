package com.gs.alloy.lakehouse.ingest.api.model.compiler;

import com.gs.alloy.lakehouse.ingest.api.accessor.protocol.IngestRelationAccessor;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.IngestDefinition;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.Dataset;
import org.eclipse.collections.api.block.procedure.Procedure2;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.CompileContext;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.ValueSpecificationPrerequisiteElementsPassBuilder;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.extension.CompilerExtension;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.extension.Processor;
import org.finos.legend.engine.protocol.pure.v1.model.context.PackageableElementPointer;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.matView.FunctionSource;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Database;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Schema;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Table;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.GeneratedAccessorTable;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.RelationalOperationElement;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.IngestColumn;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.TableAliasColumn;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.mapping.TablePtr;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class IngestCompilerExtension implements CompilerExtension
{

    @Override
    public MutableList<String> group()
    {
        return org.eclipse.collections.impl.factory.Lists.mutable.with("PackageableElement", "IngestDefinition");
    }

    @Override
    public CompilerExtension build()
    {
        return new IngestCompilerExtension();
    }

    @Override
    public Iterable<? extends Processor<?>> getExtraProcessors()
    {
        IngestCompiler ingestCompiler = new IngestCompiler();
        return Collections.singletonList(
                Processor.newProcessor(
                        IngestDefinition.class,
                        null,
                        ingestCompiler::compileFirst,
                        null,
                        ingestCompiler::compileThird,
                        this::ingestPrerequisiteElementsPass
                )
        );
    }

    @Override
    public Map<String, Procedure2<Object, Set<PackageableElementPointer>>> getExtraClassInstancePrerequisiteElementsPassProcessors()
    {
        return Maps.mutable.with("I", (obj, prerequisiteElements) ->
                {
                    IngestRelationAccessor accessor = (IngestRelationAccessor) obj;
                    prerequisiteElements.add(new PackageableElementPointer(null, accessor.path.get(0), accessor.sourceInformation));
                }
        );
    }

    private Set<PackageableElementPointer> ingestPrerequisiteElementsPass(IngestDefinition ingestDefinition, CompileContext context)
    {
        Set<PackageableElementPointer> prerequisiteElements = Sets.mutable.empty();
        if (ingestDefinition.datasets != null)
        {
            ListIterate.forEach(ingestDefinition.datasets, ds -> collectPrerequisiteElementsFromDatasets(prerequisiteElements, ds, context));
        }
        return prerequisiteElements;
    }

    private Set<PackageableElementPointer> collectPrerequisiteElementsFromDatasets(Set<PackageableElementPointer> prerequisiteElements, Dataset ds, CompileContext context)
    {
        if (ds.source instanceof FunctionSource)
        {
            ValueSpecificationPrerequisiteElementsPassBuilder builder = new ValueSpecificationPrerequisiteElementsPassBuilder(context, prerequisiteElements);
            ((FunctionSource) ds.source).function.accept(builder);
        }
        return prerequisiteElements;
    }

    // NOTE: The following helpers can be called from a Database processor housed in this module (added via another extension class)
    public static void generateAccessorTablesForImportedIngests(Database db, String dbType)
    {
        if (db.importedIngests == null || db.importedIngests.isEmpty())
        {
            return;
        }
        // Defer to the Pure function to create tables; here we only mark the shape and attach lineage holder type
        for (Schema schema : db.schemas)
        {
            for (int i = 0; i < schema.tables.size(); i++)
            {
                Table t = schema.tables.get(i);
                if (!(t instanceof GeneratedAccessorTable))
                {
                    // leave intact; generator will populate; this is a placeholder if ever needed for manual injection
                }
            }
        }
        // Real generation uses Pure dbGeneration; this stub indicates where to invoke it from the module if needed
    }

    public static RelationalOperationElement rewriteIngestColumnToTableRef(IngestColumn ingestColumn, String database, String schemaName, String tableName)
    {
        TableAliasColumn col = new TableAliasColumn();
        col.column = ingestColumn.path != null && !ingestColumn.path.isEmpty() ? ingestColumn.path.get(ingestColumn.path.size() - 1) : null;
        TablePtr ptr = new TablePtr();
        ptr._type = "Table";
        ptr.database = database;
        ptr.mainTableDb = database;
        ptr.schema = schemaName;
        ptr.table = tableName;
        col.table = ptr;
        col.tableAlias = ptr.table;
        col.sourceInformation = ingestColumn.sourceInformation;
        return col;
    }
}
