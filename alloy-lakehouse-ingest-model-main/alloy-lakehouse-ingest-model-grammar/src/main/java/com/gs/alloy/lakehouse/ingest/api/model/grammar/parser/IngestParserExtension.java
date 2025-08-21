package com.gs.alloy.lakehouse.ingest.api.model.grammar.parser;

import com.gs.alloy.lakehouse.ingest.api.model.grammar.antlr4.LakehouseIngestLexerGrammar;
import com.gs.alloy.lakehouse.ingest.api.model.grammar.antlr4.LakehouseIngestParserGrammar;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.factory.Lists;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParserContext;
import org.finos.legend.engine.language.pure.grammar.from.SectionSourceCode;
import org.finos.legend.engine.language.pure.grammar.from.SourceCodeParserInfo;
import org.finos.legend.engine.language.pure.grammar.from.extension.PureGrammarParserExtension;
import org.finos.legend.engine.language.pure.grammar.from.extension.SectionParser;
import org.finos.legend.engine.protocol.pure.m3.PackageableElement;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.section.DefaultCodeSection;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.section.Section;

import java.util.function.Consumer;

public class IngestParserExtension implements PureGrammarParserExtension
{
    public static final String NAME = "Lakehouse";


    @Override
    public MutableList<String> group()
    {
        return org.eclipse.collections.impl.factory.Lists.mutable.with("PackageableElement", "IngestDefinition");
    }

    @Override
    public Iterable<? extends SectionParser> getExtraSectionParsers()
    {
        return Lists.immutable.with(SectionParser.newParser(NAME, IngestParserExtension::parseSection));
    }

    private static Section parseSection(SectionSourceCode sectionSourceCode, Consumer<PackageableElement> elementConsumer, PureGrammarParserContext pureGrammarParserContext)
    {
        CharStream input = CharStreams.fromString(sectionSourceCode.code);
        Pair<LakehouseIngestLexerGrammar, LakehouseIngestParserGrammar> parserLexerPair = IngestParser.getLexerParser(input, sectionSourceCode.walkerSourceInformation);
        SourceCodeParserInfo parserInfo = new SourceCodeParserInfo(sectionSourceCode.code, input, sectionSourceCode.sourceInformation, sectionSourceCode.walkerSourceInformation, parserLexerPair.getOne(), parserLexerPair.getTwo(), parserLexerPair.getTwo().definition());
        DefaultCodeSection section = new DefaultCodeSection();
        section.parserName = sectionSourceCode.sectionType;
        section.sourceInformation = parserInfo.sourceInformation;
        IngestParser walker = new IngestParser(parserInfo.walkerSourceInformation, elementConsumer);
        walker.parseSection(section, (LakehouseIngestParserGrammar.DefinitionContext) parserInfo.rootContext);
        return section;
    }

}
