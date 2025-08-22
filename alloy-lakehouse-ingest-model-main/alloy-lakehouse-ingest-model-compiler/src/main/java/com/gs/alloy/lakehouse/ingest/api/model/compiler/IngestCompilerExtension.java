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
        // Resolve ingest definitions to Pure instances and call Pure generator
        CompileContext context = CompileContext.find(); // assumes thread-local or retrievable; replace with proper context passing if needed
        if (context == null)
        {
            return;
        }
        // Collect ingest definitions
        java.util.List<org.finos.legend.pure.generated.Root_meta_external_ingest_specification_metamodel_IngestDefinition> ingestDefs = new java.util.ArrayList<>();
        for (PackageableElementPointer ptr : db.importedIngests)
        {
            org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.PackageableElement pe = context.resolvePackageableElement(ptr.path, ptr.sourceInformation);
            if (pe instanceof org.finos.legend.pure.generated.Root_meta_external_ingest_specification_metamodel_IngestDefinition)
            {
                ingestDefs.add((org.finos.legend.pure.generated.Root_meta_external_ingest_specification_metamodel_IngestDefinition) pe);
            }
        }
        if (ingestDefs.isEmpty())
        {
            return;
        }
        org.finos.legend.pure.generated.Root_meta_relational_metamodel_Database pureDb = core_ingest_transformation_dbGeneration_dbGeneration.Root_meta_external_ingest_transformation_dbGeneration_generateDatabase_IngestDefinition_MANY__String_1__Database_1_(
                org.eclipse.collections.impl.list.mutable.FastList.newList(ingestDefs), dbType, context.pureModel.getExecutionSupport());

        // Build protocol schemas/tables/columns from pureDb
        // Ensure target schema exists
        org.eclipse.collections.api.list.MutableList<org.finos.legend.pure.m3.coreinstance.meta.relational.metamodel.Schema> pureSchemas = pureDb._schemas().toList();
        for (org.finos.legend.pure.m3.coreinstance.meta.relational.metamodel.Schema ps : pureSchemas)
        {
            org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Schema protoSchema = db.schemas.stream().filter(s -> s.name.equals(ps._name())).findFirst().orElse(null);
            if (protoSchema == null)
            {
                protoSchema = new org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Schema();
                protoSchema.name = ps._name();
                protoSchema.tables = new java.util.ArrayList<>();
                protoSchema.views = new java.util.ArrayList<>();
                protoSchema.tabularFunctions = new java.util.ArrayList<>();
                db.schemas.add(protoSchema);
            }
            org.eclipse.collections.api.list.MutableList<org.finos.legend.pure.m3.coreinstance.meta.relational.metamodel.relation.Table> pureTables = ps._tables().toList();
            for (org.finos.legend.pure.m3.coreinstance.meta.relational.metamodel.relation.Table pt : pureTables)
            {
                org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Table protoTable = new org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Table();
                protoTable.name = pt._name();
                protoTable.columns = new java.util.ArrayList<>();
                java.util.List<String> pk = new java.util.ArrayList<>();
                org.eclipse.collections.api.list.MutableList<org.finos.legend.pure.m3.coreinstance.meta.relational.metamodel.Column> cols = pt._columns().collect(c -> (org.finos.legend.pure.m3.coreinstance.meta.relational.metamodel.Column) c).toList();
                for (org.finos.legend.pure.m3.coreinstance.meta.relational.metamodel.Column c : cols)
                {
                    org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Column pc = new org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Column();
                    pc.name = c._name();
                    pc.nullable = c._nullable();
                    pc.type = org.finos.legend.engine.language.pure.grammar.from.datastructure.DataTypeFactory.fromPureType(c._type());
                    protoTable.columns.add(pc);
                }
                if (pt._primaryKey() != null)
                {
                    pt._primaryKey().forEach(pkCol -> pk.add(pkCol._name()));
                }
                protoTable.primaryKey = pk;
                protoSchema.tables.add(protoTable);
            }
        }
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
