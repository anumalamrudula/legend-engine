package com.gs.alloy.lakehouse.ingest.api.accessor.grammar.serializer;

import com.gs.alloy.lakehouse.ingest.api.accessor.protocol.IngestRelationAccessor;
import org.eclipse.collections.api.block.function.Function2;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.impl.factory.Lists;
import org.finos.legend.engine.language.pure.grammar.to.PureGrammarComposerContext;
import org.finos.legend.engine.language.pure.grammar.to.extension.PureGrammarComposerExtension;

import java.util.Map;

public class IngestRelationAccessorGrammarComposerExtension implements PureGrammarComposerExtension
{
    @Override
    public Map<String, Function2<Object, PureGrammarComposerContext, String>> getExtraEmbeddedPureComposers()
    {
        return Maps.mutable.with(
                "I",
                (Function2<Object, PureGrammarComposerContext, String>)
                        (o, pureGrammarComposerContext)
                                -> "#I{" + Lists.mutable.withAll(((IngestRelationAccessor) o).path).makeString(".") + (((IngestRelationAccessor) o).metadata != null && ((IngestRelationAccessor) o).metadata ? "|metadata" : "") + "}#"
        );

    }
}
