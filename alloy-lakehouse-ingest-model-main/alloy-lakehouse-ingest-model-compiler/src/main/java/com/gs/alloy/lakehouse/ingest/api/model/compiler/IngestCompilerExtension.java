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

}
