package com.gs.alloy.lakehouse.ingest.api.zFinal.compiler;

import org.eclipse.collections.api.block.function.Function2;
import org.eclipse.collections.api.block.procedure.Procedure2;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.api.tuple.Pair;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.CompileContext;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.PackageableElement;

public interface ProcessorExtension
{
    Function2<org.finos.legend.engine.protocol.pure.m3.PackageableElement, CompileContext, Pair<? extends PackageableElement, MutableSet<? extends PackageableElement>>> extractor();

    Procedure2<org.finos.legend.engine.protocol.pure.m3.PackageableElement, CompileContext> processor();

    Function2<Object, CompileContext, Iterable<org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.PackageableElement>> scanner();
}
