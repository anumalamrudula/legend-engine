package com.gs.alloy.lakehouse.ingest.api.model.grammar.parser;

import com.gs.alloy.lakehouse.ingest.api.model.grammar.SourcePlugin;
import com.gs.alloy.lakehouse.ingest.api.model.grammar.SourcePluginLoader;
import com.gs.alloy.lakehouse.ingest.api.model.grammar.antlr4.LakehouseIngestLexerGrammar;
import com.gs.alloy.lakehouse.ingest.api.model.grammar.antlr4.LakehouseIngestParserGrammar;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.IngestDefinition;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.WriteMode;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.Dataset;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.SerializedSource;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.Source;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.format.*;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.privacy.DataPrivacyClassification;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.privacy.DataSensitivity;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.AppDir;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.AppDirLevel;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.AppDirNode;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.Owner;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.preprocessing.Preprocessor;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.preprocessing.augmentation.IncludeInZOutZTimestamp;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.preprocessing.deduplication.FilterOutExactDuplicateRecords;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.preprocessing.deduplication.OverwriteOnSnapshot;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.preprocessing.deduplication.TakeMaxVersionFieldRecord;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.readMode.ReadMode;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.readMode.Undefined;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.readMode.delta.DeleteIndicator;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.readMode.delta.Delta;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.readMode.snapshot.Snapshot;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.multimap.list.MutableListMultimap;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.tuple.Tuples;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.language.pure.grammar.from.ParseTreeWalkerSourceInformation;
import org.finos.legend.engine.language.pure.grammar.from.ParserErrorListener;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParserUtility;
import org.finos.legend.engine.language.pure.grammar.from.antlr4.domain.DomainParserGrammar;
import org.finos.legend.engine.language.pure.grammar.from.domain.DomainParseTreeWalker;
import org.finos.legend.engine.protocol.pure.m3.PackageableElement;
import org.finos.legend.engine.protocol.pure.m3.extension.StereotypePtr;
import org.finos.legend.engine.protocol.pure.m3.extension.TagPtr;
import org.finos.legend.engine.protocol.pure.m3.extension.TaggedValue;
import org.finos.legend.engine.protocol.pure.m3.valuespecification.constant.datatype.primitive.CInteger;
import org.finos.legend.engine.protocol.pure.m3.valuespecification.constant.datatype.primitive.CString;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.section.DefaultCodeSection;
import org.finos.legend.engine.protocol.pure.m3.relation.RelationType;
import org.finos.legend.engine.shared.core.operational.errorManagement.EngineException;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class IngestParser
{
    private final ParseTreeWalkerSourceInformation walkerSourceInformation;

    private final Consumer<PackageableElement> elementConsumer;

    public IngestParser(ParseTreeWalkerSourceInformation walkerSourceInformation, Consumer<PackageableElement> elementConsumer)
    {
        this.walkerSourceInformation = walkerSourceInformation;
        this.elementConsumer = elementConsumer;
    }

    public IngestParser()
    {
        this(new ParseTreeWalkerSourceInformation.Builder("sourceId", 0, 0).build(), null);
    }


    public void parseSection(DefaultCodeSection section, LakehouseIngestParserGrammar.DefinitionContext ctx)
    {
        ctx.ingest().stream().map(this::transform).peek(e -> section.elements.add(e.getPath())).forEach(this.elementConsumer);

    }

    public IngestDefinition parse(String txt)
    {
        return transform(this.visit(txt));
    }

    public LakehouseIngestParserGrammar.IngestContext visit(String code)
    {
        CharStream input = CharStreams.fromString(code);
        Pair<LakehouseIngestLexerGrammar, LakehouseIngestParserGrammar> parserLexerPair = IngestParser.getLexerParser(input, this.walkerSourceInformation);
        return parserLexerPair.getTwo().ingest();
    }

    public static Pair<LakehouseIngestLexerGrammar, LakehouseIngestParserGrammar> getLexerParser(CharStream input, ParseTreeWalkerSourceInformation walkerSourceInformation)
    {
        ParserErrorListener errorListener = new ParserErrorListener(walkerSourceInformation);
        LakehouseIngestLexerGrammar lexer = new LakehouseIngestLexerGrammar(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        LakehouseIngestParserGrammar parser = new LakehouseIngestParserGrammar(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);
        return Tuples.pair(lexer, parser);
    }

    private IngestDefinition transform(LakehouseIngestParserGrammar.IngestContext ctx)
    {
        IngestDefinition result = new IngestDefinition()
                ._writeMode(transform(ctx.writeMode()))
                ._readMode(transform(ctx.readMode()))
                ._datasets(ListIterate.collect(ctx.dataSet(), this::transform))
                ._stereotypes(ctx.stereotypes() == null ? Lists.mutable.empty() : this.visitStereotypes(ctx.stereotypes()))
                ._taggedValues(ctx.taggedValues() == null ? Lists.mutable.empty() : this.visitTaggedValues(ctx.taggedValues()));
        result.name = PureGrammarParserUtility.fromIdentifier(ctx.qualifiedName().identifier());
        result._package = ctx.qualifiedName().packagePath() == null ? "" : PureGrammarParserUtility.fromPath(ctx.qualifiedName().packagePath().identifier());
        if (ctx.group != null)
        {
            result._datasetGroup(ctx.group.getText());
        }
        result._owner(transform(ctx.ownerBackComp()));
        result.sourceInformation = this.walkerSourceInformation.getSourceInformation(ctx);
        return result;
    }

    public List<StereotypePtr> visitStereotypes(LakehouseIngestParserGrammar.StereotypesContext ctx)
    {
        return ListIterate.collect(ctx.stereotype(), stereotypeContext ->
        {
            StereotypePtr stereotypePtr = new StereotypePtr();
            stereotypePtr.profile = PureGrammarParserUtility.fromQualifiedName(stereotypeContext.qualifiedName().packagePath() == null ? Collections.emptyList() : stereotypeContext.qualifiedName().packagePath().identifier(), stereotypeContext.qualifiedName().identifier());
            stereotypePtr.value = PureGrammarParserUtility.fromIdentifier(stereotypeContext.identifier());
            stereotypePtr.profileSourceInformation = this.walkerSourceInformation.getSourceInformation(stereotypeContext.qualifiedName());
            stereotypePtr.sourceInformation = this.walkerSourceInformation.getSourceInformation(stereotypeContext.identifier());
            return stereotypePtr;
        });
    }

    private List<TaggedValue> visitTaggedValues(LakehouseIngestParserGrammar.TaggedValuesContext ctx)
    {
        return ListIterate.collect(ctx.taggedValue(), taggedValueContext ->
        {
            TaggedValue taggedValue = new TaggedValue();
            TagPtr tagPtr = new TagPtr();
            taggedValue.tag = tagPtr;
            tagPtr.profile = PureGrammarParserUtility.fromQualifiedName(taggedValueContext.qualifiedName().packagePath() == null ? Collections.emptyList() : taggedValueContext.qualifiedName().packagePath().identifier(), taggedValueContext.qualifiedName().identifier());
            tagPtr.value = PureGrammarParserUtility.fromIdentifier(taggedValueContext.identifier());
            taggedValue.value = PureGrammarParserUtility.fromGrammarString(taggedValueContext.STRING().getText(), true);
            taggedValue.tag.profileSourceInformation = this.walkerSourceInformation.getSourceInformation(taggedValueContext.qualifiedName());
            taggedValue.tag.sourceInformation = this.walkerSourceInformation.getSourceInformation(taggedValueContext.identifier());
            taggedValue.sourceInformation = this.walkerSourceInformation.getSourceInformation(taggedValueContext);
            return taggedValue;
        });
    }

    private Owner transform(LakehouseIngestParserGrammar.OwnerBackCompContext ownerBackCompContext)
    {
        if (ownerBackCompContext != null)
        {
            if (ownerBackCompContext.deploymentId() != null)
            {
                AppDirNode production = new AppDirNode()._level(AppDirLevel.DEPLOYMENT)._appDirId(Long.parseLong(ownerBackCompContext.deploymentId().id.getText()));
                return new AppDir()._production(production);
            }
            else if (ownerBackCompContext.owner() != null)
            {
                MutableMap<String, String> keyValues = Maps.mutable.empty();
                if (ownerBackCompContext.owner().keyValues() != null)
                {
                    ListIterate.forEach(ownerBackCompContext.owner().keyValues().keyValue(), x -> keyValues.put(x.VALID_STRING().getText(), PureGrammarParserUtility.fromGrammarString(x.STRING().getText(), true)));
                }
                MutableMap<String, AppDirNode> nodes = Maps.mutable.empty();
                keyValues.forEachKey(k ->
                {
                    if (k.equals("production") || k.equals("prodParallel"))
                    {
                        nodes.put(k, new AppDirNode()
                                ._level(AppDirLevel.DEPLOYMENT)
                                ._appDirId(Long.valueOf(keyValues.get(k))));
                    }
                    else
                    {
                        throw new EngineException(k + " is unknown. Possible values are 'production' and 'prodParallel'");
                    }
                });
                return new AppDir()._production(nodes.get("production"))._prodParallel(nodes.get("prodParallel"));
            }
        }
        return null;
    }

    private Dataset transform(LakehouseIngestParserGrammar.DataSetContext x)
    {
        Source source = null;

        for (SourcePlugin sourcePlugin : SourcePluginLoader.extensions())
        {
            source = sourcePlugin.parseSource(x);
            if (source != null)
            {
                break;
            }
        }

        if (source == null && x.relationType() != null)
        {
            SerializedSource serializedSource = new SerializedSource();
            serializedSource._schema(transform(x.relationType()));
            source = serializedSource;
        }

        if (source == null)
        {
            throw new RuntimeException("The dataset source can't be understood. Available source plugins:[" + ListIterate.collect(SourcePluginLoader.extensions(), l -> l.getClass().getSimpleName()).makeString(", ") + "]");
        }

        Dataset result = new Dataset()
                ._name(x.name().VALID_STRING().getText())
                ._source(source)
                ._privacyClassification(new DataPrivacyClassification()._sensitivity(transform(x.sensitivity())));

        // Manage optionals
        final Source _source = source;
        return ListIterate.injectInto(result, x.optionals(), (a, b) ->
                {
                    if (b.pk() != null)
                    {
                        return result._primaryKey(ListIterate.collect(b.pk().VALID_STRING(), ParseTree::getText));
                    }
                    if (b.partition() != null)
                    {
                        return result._ingestPartitionColumns(ListIterate.collect(b.partition().VALID_STRING(), ParseTree::getText));
                    }
                    if (b.storageLayoutCluster() != null)
                    {
                        return result._storageLayoutClusterColumns(ListIterate.collect(b.storageLayoutCluster().VALID_STRING(), ParseTree::getText));
                    }
                    if (b.storageLayoutPartition() != null)
                    {
                        return result._storageLayoutPartitionColumns(ListIterate.collect(b.storageLayoutPartition().VALID_STRING(), ParseTree::getText));
                    }
                    if (b.formatOverride() != null)
                    {
                        ((SerializedSource) _source)._format(transform(b.formatOverride().format()));
                        return result;
                    }
                    if (b.preprocessors() != null)
                    {
                        return result._preprocessors(ListIterate.collect(b.preprocessors().preprocessor(), this::transform));
                    }
                    return a;
                }
        );
    }

    private Preprocessor transform(LakehouseIngestParserGrammar.PreprocessorContext preprocessor)
    {
        if (preprocessor.FILTER_OUT_EXACT_DUPLICATE_RECORDS() != null)
        {
            return new FilterOutExactDuplicateRecords();
        }
        else if (preprocessor.TAKE_MAX_VERSION_FIELD_RECORD() != null)
        {
            MutableMap<String, LakehouseIngestParserGrammar.GenericNameValueContext> index = validateParameters(preprocessor.genericParameters(), Lists.mutable.with("versionField", "requireIncreasingVersion"));
            TakeMaxVersionFieldRecord takeMaxVersionFieldRecordWithDefault = new TakeMaxVersionFieldRecord();
            return new TakeMaxVersionFieldRecord()._versionField(string(index, "versionField", takeMaxVersionFieldRecordWithDefault.versionField))
                    ._requireIncreasingVersion(Boolean.parseBoolean(string(index, "requireIncreasingVersion", takeMaxVersionFieldRecordWithDefault.requireIncreasingVersion.toString())));
        }
        else if (preprocessor.INCLUDE_INZ_OUTZ_TIMESTAMP() != null)
        {
            return new IncludeInZOutZTimestamp();
        }
        else if (preprocessor.OVERWRITE_ON_SNAPSHOT() != null)
        {
            return new OverwriteOnSnapshot();
        }
        else
        {
            throw new RuntimeException("Error!");
        }
    }

    private MessageFormat transform(LakehouseIngestParserGrammar.FormatContext format)
    {
        if (format.formatType().AVRO() != null)
        {
            return new AvroMessageFormat();
        }
        else if (format.formatType().JSON() != null)
        {
            return new JsonMessageFormat();
        }
        else if (format.formatType().PARQUET() != null)
        {
            return new ParquetMessageFormat();
        }
        else if (format.formatType().CSV() != null)
        {
            MutableMap<String, LakehouseIngestParserGrammar.GenericNameValueContext> index = validateParameters(format.genericParameters(), Lists.mutable.with("fieldDelimiter", "headerRowsToSkipCount", "quoteCharacter", "escapeCharacter", "recordDelimiter"));
            CsvMessageFormat resultWithDefaults = new CsvMessageFormat();
            return resultWithDefaults
                    ._fieldDelimiter(string(index, "fieldDelimiter", resultWithDefaults.fieldDelimiter))
                    ._recordDelimiter(string(index, "recordDelimiter", resultWithDefaults.recordDelimiter))
                    ._escapeCharacter(string(index, "escapeCharacter", resultWithDefaults.escapeCharacter))
                    ._quoteCharacter(string(index, "quoteCharacter", resultWithDefaults.quoteCharacter))
                    ._headerRowsToSkipCount(_long(index, "headerRowsToSkipCount", resultWithDefaults.headerRowsToSkipCount));
        }
        return null;
    }

    private static MutableMap<String, LakehouseIngestParserGrammar.GenericNameValueContext> validateParameters(LakehouseIngestParserGrammar.GenericParametersContext genericParameters, MutableList<String> validKeys)
    {
        MutableMap<String, LakehouseIngestParserGrammar.GenericNameValueContext> index = genericParameters == null ? Maps.mutable.empty() : ListIterate.groupByUniqueKey(genericParameters.genericNameValue(), x -> x.VALID_STRING().getText());
        MutableSet<String> allKeys = index.keysView().toSet();
        validKeys.forEach(allKeys::remove);
        if (!allKeys.isEmpty())
        {
            throw new EngineException("'" + allKeys.makeString(", ") + "'" + (allKeys.size() == 1 ? " is not a value key." : " are not valid keys.") + " Valid keys are '" + validKeys.makeString(",") + "'");
        }
        return index;
    }

    private Long _long(MutableMap<String, LakehouseIngestParserGrammar.GenericNameValueContext> index, String key, Long _default)
    {
        LakehouseIngestParserGrammar.GenericNameValueContext res = index.get(key);
        return res == null ? _default : ((CInteger) new DomainParseTreeWalker(this.walkerSourceInformation, null, false).instanceLiteral((DomainParserGrammar.InstanceLiteralContext) copy(res.instanceLiteral(), LakehouseIngestParserGrammar.class, DomainParserGrammar.class), false)).value;
    }

    private String string(MutableMap<String, LakehouseIngestParserGrammar.GenericNameValueContext> index, String key, String _default)
    {
        LakehouseIngestParserGrammar.GenericNameValueContext res = index.get(key);
        return res == null ? _default : ((CString) new DomainParseTreeWalker(this.walkerSourceInformation, null, false).instanceLiteral((DomainParserGrammar.InstanceLiteralContext) copy(res.instanceLiteral(), LakehouseIngestParserGrammar.class, DomainParserGrammar.class), false)).value;
    }

    private DataSensitivity transform(LakehouseIngestParserGrammar.SensitivityContext sensitivity)
    {
        if (sensitivity.DP00() != null)
        {
            return DataSensitivity.DP00;
        }
        else if (sensitivity.DP10() != null)
        {
            return DataSensitivity.DP10;
        }
        else if (sensitivity.DP20() != null)
        {
            return DataSensitivity.DP20;
        }
        else if (sensitivity.DP30() != null)
        {
            return DataSensitivity.DP30;
        }
        throw new RuntimeException("Error!");
    }

    private RelationType transform(LakehouseIngestParserGrammar.RelationTypeContext ctx)
    {
        return new DomainParseTreeWalker(this.walkerSourceInformation, null, false).relationType((DomainParserGrammar.RelationTypeContext) copy(ctx, LakehouseIngestParserGrammar.class, DomainParserGrammar.class));
    }

    private ReadMode transform(LakehouseIngestParserGrammar.ReadModeContext readModeContext)
    {
        ReadMode result;
        if (readModeContext.snapshot() != null)
        {
            result = new Snapshot();
        }
        else if (readModeContext.delta() != null)
        {
            LakehouseIngestParserGrammar.DeleteColumnContext delCol = readModeContext.delta().deleteColumn();
            result = new Delta()
                    ._deleteIndicator(
                            delCol == null ?
                                    null :
                                    new DeleteIndicator()
                                            ._deleteField(delCol.VALID_STRING().getText())
                                            ._deleteValues(ListIterate.collect(readModeContext.delta().deleteValue(), x -> x.VALID_STRING().getText())));
        }
        else if (readModeContext.undefined() != null)
        {
            result = new Undefined();
        }
        else
        {
            throw new RuntimeException("Error!");
        }
        if (readModeContext.format() != null)
        {
            result._format(transform(readModeContext.format()));
        }
        return result;
    }

    private WriteMode transform(LakehouseIngestParserGrammar.WriteModeContext writeModeContext)
    {
        if (writeModeContext.APPEND_ONLY() != null)
        {
            return WriteMode.APPEND_ONLY;
        }
        else if (writeModeContext.BATCH_MILESTONED() != null)
        {
            return WriteMode.BATCH_MILESTONED;
        }
        else if (writeModeContext.OVERWRITE() != null)
        {
            return WriteMode.OVERWRITE;
        }
        throw new RuntimeException("Error!");
    }


    public static ParseTree copy(ParseTree srcObj, Class<?> srcClass, Class<?> targetClass)
    {
        try
        {
            MutableMap<String, Integer> target = org.eclipse.collections.api.factory.Maps.mutable.empty();
            MutableMap<Integer, String> source = org.eclipse.collections.api.factory.Maps.mutable.empty();
            for (Field x : org.eclipse.collections.impl.factory.Lists.mutable.with(targetClass.getFields()).select(x -> !x.getName().startsWith("RULE_") && x.getType() == Integer.TYPE))
            {
                target.put(x.getName(), x.getInt(null));
            }
            for (Field x : org.eclipse.collections.impl.factory.Lists.mutable.with(srcClass.getFields()).select(x -> !x.getName().startsWith("RULE_") && x.getType() == Integer.TYPE))
            {
                source.put(x.getInt(null), x.getName());
            }
            return copy(srcObj, org.eclipse.collections.impl.factory.Lists.mutable.with(targetClass.getDeclaredClasses()).groupBy(Class::getSimpleName), source, target);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

    }


    private static ParseTree copy(ParseTree src, MutableListMultimap<String, Class<?>> targetClasses, MutableMap<Integer, String> source, MutableMap<String, Integer> target) throws Exception
    {
        if (src instanceof ParserRuleContext)
        {
            Class<?> cl = targetClasses.get(src.getClass().getSimpleName()).get(0);
            ParserRuleContext _src = (ParserRuleContext) src;
            ParserRuleContext res = (ParserRuleContext) cl.getDeclaredConstructor(ParserRuleContext.class, Integer.TYPE).newInstance((ParserRuleContext) _src.parent, _src.invokingState);
            res.parent = _src.parent;
            res.invokingState = _src.invokingState;
            res.start = _src.start;
            res.stop = _src.stop;
            if (_src.children != null)
            {
                for (ParseTree pr : _src.children)
                {
                    res.addAnyChild(copy(pr, targetClasses, source, target));
                }
            }
            return res;
        }
        if (src instanceof TerminalNodeImpl)
        {
            CommonToken tk = new CommonToken(((TerminalNode) src).getSymbol());
            tk.setType(target.get(source.get(((TerminalNode) src).getSymbol().getType())));
            return new TerminalNodeImpl(tk);
        }
        if (src == null)
        {
            return null;
        }
        throw new RuntimeException("YO " + src.getClass().getName());
    }

}
