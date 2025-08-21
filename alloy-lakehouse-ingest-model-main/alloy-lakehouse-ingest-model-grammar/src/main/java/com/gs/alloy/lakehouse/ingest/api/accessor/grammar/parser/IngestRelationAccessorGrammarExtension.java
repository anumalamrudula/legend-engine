package com.gs.alloy.lakehouse.ingest.api.accessor.grammar.parser;

import com.gs.alloy.lakehouse.ingest.api.accessor.protocol.IngestRelationAccessor;
import org.eclipse.collections.api.factory.Lists;
import org.finos.legend.engine.language.pure.grammar.from.ParseTreeWalkerSourceInformation;
import org.finos.legend.engine.language.pure.grammar.from.extension.EmbeddedPureParser;
import org.finos.legend.engine.language.pure.grammar.from.extension.PureGrammarParserExtension;
import org.finos.legend.engine.language.pure.grammar.from.extension.PureGrammarParserExtensions;
import org.finos.legend.engine.protocol.pure.m3.SourceInformation;

public class IngestRelationAccessorGrammarExtension implements PureGrammarParserExtension
{
    @Override
    public Iterable<? extends EmbeddedPureParser> getExtraEmbeddedPureParsers()
    {
        return Lists.mutable.with(new EmbeddedPureParser()
        {
            @Override
            public String getType()
            {
                return "I";
            }

            @Override
            public Object parse(String _code, ParseTreeWalkerSourceInformation walkerSourceInformation, SourceInformation sourceInformation, PureGrammarParserExtensions extensions)
            {
                String code = _code.trim();
                IngestRelationAccessor ingestRelationAccessor = new IngestRelationAccessor();
                boolean hasMetadata = code.endsWith("|metadata");
                String pathOnly = hasMetadata ? code.substring(0, code.length() - "|metadata".length()) : code;
                ingestRelationAccessor.path = Lists.mutable.with(pathOnly.split("\\."));
                ingestRelationAccessor.metadata =  hasMetadata;
                ingestRelationAccessor.sourceInformation = sourceInformation;
                return ingestRelationAccessor;
            }
        });
    }


}
