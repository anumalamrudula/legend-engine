package com.gs.alloy.lakehouse.ingest.api.model.grammar;

import com.gs.alloy.lakehouse.ingest.api.model.grammar.antlr4.LakehouseIngestParserGrammar;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.Source;

public interface SourcePlugin
{
    Source parseSource(LakehouseIngestParserGrammar.DataSetContext x);

    String renderSource(Source source);
}
