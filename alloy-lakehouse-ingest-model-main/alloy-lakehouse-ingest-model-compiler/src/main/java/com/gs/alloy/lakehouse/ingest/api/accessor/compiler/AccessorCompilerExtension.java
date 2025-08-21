package com.gs.alloy.lakehouse.ingest.api.accessor.compiler;

import com.gs.alloy.lakehouse.ingest.api.accessor.protocol.IngestRelationAccessor;
import org.eclipse.collections.api.block.function.Function3;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.CompileContext;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.ProcessingContext;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.extension.CompilerExtension;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.extension.Processor;
import org.finos.legend.engine.protocol.pure.v1.model.context.EngineErrorType;
import org.finos.legend.engine.shared.core.operational.errorManagement.EngineException;
import org.finos.legend.pure.generated.*;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.PackageableElement;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.type.generics.GenericType;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.valuespecification.ValueSpecification;
import org.finos.legend.pure.m3.navigation.M3Paths;

import java.util.Map;

public class AccessorCompilerExtension implements CompilerExtension
{
    @Override
    public MutableList<String> group()
    {
        return org.eclipse.collections.impl.factory.Lists.mutable.with("ClassInstance", "IngestRelationAccessor");
    }

    @Override
    public Map<String, Function3<Object, CompileContext, ProcessingContext, ValueSpecification>> getExtraClassInstanceProcessors()
    {
        return Maps.mutable.with("I", new Function3<Object, CompileContext, ProcessingContext, ValueSpecification>()
        {
            @Override
            public ValueSpecification value(Object o, CompileContext _compileContext, ProcessingContext processingContext)
            {
                IngestRelationAccessor accessor = (IngestRelationAccessor) o;

                CompileContext compileContext = new CompileContext.Builder(_compileContext).withImports(Sets.immutable.with("meta::pure::precisePrimitives")).build();

                if (accessor.path.size() != 2)
                {
                    throw new EngineException("An Ingest Specification accessor has to be of the form: 'package::IngestSpec.datasetName'", accessor.sourceInformation, EngineErrorType.COMPILATION);
                }

                PackageableElement packageableElement = compileContext.resolvePackageableElement(accessor.path.get(0), accessor.sourceInformation);

                if (!(packageableElement instanceof Root_meta_external_ingest_specification_metamodel_IngestDefinition))
                {
                    throw new EngineException("The element '" + accessor.path.get(0) + "' is not an Ingest Definition", accessor.sourceInformation, EngineErrorType.COMPILATION);
                }

                Root_meta_external_ingest_specification_metamodel_IngestDefinition definition = (Root_meta_external_ingest_specification_metamodel_IngestDefinition) packageableElement;

                Root_meta_external_ingest_specification_metamodel_dataset_Dataset dataset = definition._datasets().detect(c -> accessor.path.get(1).equals(c._name()));

                if (dataset == null)
                {
                    throw new EngineException("The dataset '" + accessor.path.get(1) + "' can't be found in the IngestDefinition '" + accessor.path.get(0) + "'", accessor.sourceInformation, EngineErrorType.COMPILATION);
                }

                GenericType genericType = new Root_meta_pure_metamodel_type_generics_GenericType_Impl("", null, compileContext.pureModel.getClass(M3Paths.GenericType))
                        ._rawType(compileContext.pureModel.getType("meta::external::ingest::accessor::IngestRelationAccessor"))
                        ._typeArguments(FastList.newListWith(
                                        new Root_meta_pure_metamodel_type_generics_GenericType_Impl("", null, compileContext.pureModel.getClass(M3Paths.GenericType))
                                                ._rawType(accessor.metadata ?
                                                        core_ingest_transformation_dbGeneration_dbGeneration.Root_meta_external_ingest_transformation_dbGeneration_buildMetadataRelationType__RelationType_1_(_compileContext.getExecutionSupport())
                                                        : dataset._source().relationType(compileContext.pureModel.getExecutionSupport()))
                                )
                        );

                return new Root_meta_pure_metamodel_valuespecification_InstanceValue_Impl("", null, compileContext.pureModel.getClass(M3Paths.InstanceValue))
                        ._genericType(genericType)
                        ._multiplicity(compileContext.pureModel.getMultiplicity("one"))
                        ._values(Lists.mutable.with(
                                        new Root_meta_external_ingest_accessor_IngestRelationAccessor_Impl("")
                                                ._classifierGenericType(genericType)
                                                ._ingestDefinition(definition)
                                                ._dataset(dataset)
                                                ._metadata(accessor.metadata)
                                )
                        );
            }
        });
    }

    @Override
    public CompilerExtension build()
    {
        return new AccessorCompilerExtension();
    }

    @Override
    public Iterable<? extends Processor<?>> getExtraProcessors()
    {
        return Lists.mutable.empty();
    }

    @Override
    public MutableMap<String, MutableSet<String>> getExtraSubtypesForFunctionMatching()
    {
        return org.eclipse.collections.impl.factory.Maps.mutable.with("cov_relation_Relation", Sets.mutable.with("IngestRelationAccessor"));
    }
}
