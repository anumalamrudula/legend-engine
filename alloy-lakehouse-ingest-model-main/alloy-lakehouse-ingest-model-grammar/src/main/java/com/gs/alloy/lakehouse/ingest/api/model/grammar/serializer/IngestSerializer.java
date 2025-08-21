package com.gs.alloy.lakehouse.ingest.api.model.grammar.serializer;

import com.gs.alloy.lakehouse.ingest.api.model.grammar.SourcePlugin;
import com.gs.alloy.lakehouse.ingest.api.model.grammar.SourcePluginLoader;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.IngestDefinition;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.WriteMode;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.Dataset;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.SerializedSource;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.Source;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.format.*;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.privacy.DataPrivacyClassification;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.AppDir;
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
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.language.pure.grammar.to.HelperDomainGrammarComposer;
import org.finos.legend.engine.protocol.pure.m3.multiplicity.Multiplicity;
import org.finos.legend.engine.protocol.pure.m3.relation.RelationType;
import org.finos.legend.engine.protocol.pure.m3.type.Type;
import org.finos.legend.engine.protocol.pure.m3.type.generics.GenericType;
import org.finos.legend.engine.protocol.pure.m3.valuespecification.ValueSpecification;
import org.finos.legend.engine.protocol.pure.m3.valuespecification.constant.PackageableType;
import org.finos.legend.engine.protocol.pure.m3.valuespecification.constant.datatype.primitive.CInteger;
import org.finos.legend.engine.protocol.pure.m3.valuespecification.constant.datatype.primitive.CString;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class IngestSerializer
{
    public String serialize(IngestDefinition ingestDefinition)
    {
        String id = ingestDefinition._package == null || ingestDefinition._package.isEmpty() ? ingestDefinition.name : ingestDefinition._package + "::" + ingestDefinition.name;
        return "Ingest" + " " + HelperDomainGrammarComposer.renderAnnotations(ingestDefinition.stereotypes, ingestDefinition.taggedValues) + id +
                Lists.mutable
                        .withAll(serialize(ingestDefinition.writeMode))
                        .withAll(serialize(ingestDefinition.readMode))
                        .withAll(serializeDeployment(ingestDefinition.owner))
                        .withAll(serializeGroup(ingestDefinition.datasetGroup))
                        .makeString(" ", " ", "") + "\n" +
                "[\n" +
                "   " + ListIterate.collect(ingestDefinition.datasets, this::serialize).makeString("\n\n   ") + "\n" +
                "]\n";
    }

    private MutableList<String> serializeGroup(String datasetGroup)
    {
        IngestDefinition _default = new IngestDefinition();
        if (datasetGroup == null || Objects.equals(datasetGroup, _default.datasetGroup))
        {
            return Lists.mutable.empty();
        }
        return Lists.mutable.with("group=" + datasetGroup);
    }

    private MutableList<String> serializeDeployment(Owner owner)
    {
        if (owner == null)
        {
            return Lists.mutable.empty();
        }
        else if (owner instanceof AppDir)
        {
            AppDir appDir = (AppDir) owner;
            if (appDir.prodParallel == null || (appDir.production != null && appDir.production.appDirId.equals(appDir.prodParallel.appDirId)))
            {
                return Lists.mutable.with("deploymentId=" + appDir.production.appDirId);
            }
            else
            {
                return Lists.mutable.with("owner=AppDir(" +
                        Lists.mutable.with(
                                (appDir.production == null ? null : "production='" + appDir.production.appDirId + "'"),
                                "prodParallel='" + appDir.prodParallel.appDirId + "'"
                        ).select(Objects::nonNull).makeString(", ") +
                        ")"
                );
            }
        }
        else
        {
            throw new RuntimeException(owner.getClass() + " is not supported yet");
        }
    }

    private String serialize(Dataset x)
    {
        String newlineWithIndent = "\n   ";
        return x.name +
                serialize(x.source) + newlineWithIndent +
                serialize(x.privacyClassification) +
                (!(x.source instanceof SerializedSource) || ((SerializedSource) x.source).format == null ? "" : newlineWithIndent + "format=" + serialize(((SerializedSource) x.source).format)) +
                (x.primaryKey.isEmpty() ? "" : newlineWithIndent + "pk=[" + Lists.mutable.withAll(x.primaryKey)
                        .makeString(",") + "]") +
                (x.ingestPartitionColumns.isEmpty() ? "" : newlineWithIndent + "partition=[" + Lists.mutable.withAll(
                        x.ingestPartitionColumns).makeString(", ") + "]") +
                (x.storageLayoutClusterColumns.isEmpty() ? "" : newlineWithIndent + "storageLayoutCluster=[" + Lists.mutable.withAll(
                        x.storageLayoutClusterColumns).makeString(", ") + "]") +
                (x.storageLayoutPartitionColumns.isEmpty() ? "" : newlineWithIndent + "storageLayoutPartition=[" + Lists.mutable.withAll(
                        x.storageLayoutPartitionColumns).makeString(", ") + "]") +
                (x.preprocessors.isEmpty() ? "" : newlineWithIndent + "preprocessors=[" + newlineWithIndent + "   " +
                        ListIterate.collect(x.preprocessors, this::serialize)
                                .makeString("," + newlineWithIndent + "   ") +
                        newlineWithIndent + "]") +
                ";";
    }

    private String serialize(Source source)
    {
        for (SourcePlugin sourcePlugin : SourcePluginLoader.extensions())
        {
            String result = sourcePlugin.renderSource(source);
            if (result != null)
            {
                return result;
            }
        }

        if (source instanceof SerializedSource)
        {
            return serialize(((SerializedSource) source).schema);
        }

        throw new RuntimeException(source.getClass() + " is not supported yet");
    }

    private Object serialize(Preprocessor preprocessor)
    {
        if (preprocessor instanceof TakeMaxVersionFieldRecord)
        {
            TakeMaxVersionFieldRecord casted = (TakeMaxVersionFieldRecord) preprocessor;
            List<String> genericNameValueParis = new ArrayList<>();
            if (casted.versionField != null)
            {
                genericNameValueParis.add("versionField='" + escape(casted.versionField) + "'");
            }
            if (casted.requireIncreasingVersion != null && casted.requireIncreasingVersion)
            {
                genericNameValueParis.add("requireIncreasingVersion='true'");
            }
            return "TakeMaxVersionFieldRecord" + (genericNameValueParis.isEmpty()
                    ? ""
                    : ("{" + String.join(", ", genericNameValueParis) + "}"));
        }
        else if (preprocessor instanceof FilterOutExactDuplicateRecords)
        {
            return "FilterOutExactDuplicateRecords";
        }
        else if (preprocessor instanceof IncludeInZOutZTimestamp)
        {
            return "IncludeInZOutZTimestamp";
        }
        else if (preprocessor instanceof OverwriteOnSnapshot)
        {
            return "OverwriteOnSnapshot";
        }
        throw new RuntimeException(String.format("Unknown preprocessor: %s", preprocessor));
    }

    private String serialize(MessageFormat messageFormat)
    {
        if (messageFormat instanceof AvroMessageFormat)
        {
            return "AVRO";
        }
        else if (messageFormat instanceof CsvMessageFormat)
        {
            CsvMessageFormat csv = (CsvMessageFormat) messageFormat;
            CsvMessageFormat messageWithDefaults = new CsvMessageFormat();
            MutableList<String> params = Lists.mutable
                    .with(csv.recordDelimiter == null || Objects.equals(csv.recordDelimiter,
                            messageWithDefaults.recordDelimiter) ? null : "recordDelimiter = '" + escape(
                            csv.recordDelimiter) + "'")
                    .with(csv.fieldDelimiter == null || Objects.equals(csv.fieldDelimiter,
                            messageWithDefaults.fieldDelimiter) ? null : "fieldDelimiter = '" + escape(
                            csv.fieldDelimiter) + "'")
                    .with(csv.escapeCharacter == null || Objects.equals(csv.escapeCharacter,
                            messageWithDefaults.escapeCharacter) ? null : "escapeCharacter = '" + escape(
                            csv.escapeCharacter) + "'")
                    .with(csv.quoteCharacter == null || Objects.equals(csv.quoteCharacter,
                            messageWithDefaults.quoteCharacter) ? null : "quoteCharacter = '" + escape(
                            csv.quoteCharacter) + "'")
                    .with(csv.headerRowsToSkipCount == null || Objects.equals(csv.headerRowsToSkipCount,
                            messageWithDefaults.headerRowsToSkipCount) ? null : "headerRowsToSkipCount = " + csv.headerRowsToSkipCount)
                    .select(Objects::nonNull);
            return "CSV" + (params.isEmpty() ? "" : params.makeString("{", ", ", "}"));
        }
        else if (messageFormat instanceof ParquetMessageFormat)
        {
            return "PARQUET";
        }
        else if (messageFormat instanceof JsonMessageFormat)
        {
            return "JSON";
        }
        throw new RuntimeException(String.format("Unknown message format: %s", messageFormat));
    }

    private String escape(String value)
    {
        return value.replace("\n", "\\n").replace("'", "\\'");
    }

    private String serialize(DataPrivacyClassification privacyClassification)
    {
        return privacyClassification.sensitivity.getName();
    }

    private String serialize(RelationType relationType)
    {
        return "(\n      " + ListIterate.collect(relationType.columns, x -> x.name + ": " + serialize(x.genericType) +
                (x.multiplicity.equals(Multiplicity.ZERO_ONE) ? "" : "[" + HelperDomainGrammarComposer.renderMultiplicity(
                        x.multiplicity) + "]")).makeString(",\n      ") + "\n   )";
    }

    private String serialize(GenericType genericType)
    {
        return serialize(genericType.rawType) +
                ((genericType.typeVariableValues.isEmpty()) ? "" : "(" + ListIterate.collect(
                        genericType.typeVariableValues,
                        this::serialize).makeString(",") + ")");
    }

    private Object serialize(ValueSpecification x)
    {
        if (x instanceof CString)
        {
            return "'" + ((CString) x).value + "'";
        }
        else if (x instanceof CInteger)
        {
            return ((CInteger) x).value;
        }
        throw new RuntimeException("Not Supported");
    }

    private String serialize(Type type)
    {
        if (type instanceof PackageableType)
        {
            return ((PackageableType) type).fullPath;
        }
        throw new RuntimeException("Not Supported");
    }

    private MutableList<String> serialize(ReadMode mode)
    {
        String base;
        if (mode == null)
        {
            return Lists.mutable.empty();
        }
        else if (mode instanceof Snapshot)
        {
            base = "Snapshot";
        }
        else if (mode instanceof Delta)
        {
            DeleteIndicator indicator = ((Delta) mode).deleteIndicator;
            base = "Delta" + (indicator == null ? "" : "(" + indicator.deleteField + "," + Lists.mutable.withAll(
                    indicator.deleteValues).makeString("[", ",", "]") + ")");
        }
        else if (mode instanceof Undefined)
        {
            base = "Undefined";
        }
        else
        {
            throw new RuntimeException(String.format("Unknown read mode: %s", mode));
        }
        return Lists.mutable.with(base + (mode.format == null ? "" : "<" + serialize(mode.format) + ">"));
    }

    private MutableList<String> serialize(WriteMode writeMode)
    {
        switch (writeMode)
        {
            case BATCH_MILESTONED:
            {
                return Lists.mutable.with("BatchMilestoned");
            }
            case OVERWRITE:
            {
                return Lists.mutable.with("Overwrite");
            }
            case APPEND_ONLY:
            {
                return Lists.mutable.with("AppendOnly");
            }
        }
        throw new RuntimeException(String.format("Unknown write mode: %s", writeMode));
    }
}
