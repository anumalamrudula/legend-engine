package com.gs.alloy.lakehouse.ingest.api.model.compiler;

import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.IngestDefinition;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.Source;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.CompileContext;
import org.finos.legend.pure.generated.Root_meta_external_ingest_specification_metamodel_IngestDefinition;
import org.finos.legend.pure.generated.Root_meta_external_ingest_specification_metamodel_dataset_Source;

import java.util.Collection;

public interface SourceCompilerExtension
{
    Root_meta_external_ingest_specification_metamodel_dataset_Source compile(Source source, CompileContext _context);
}
