package com.gs.alloy.lakehouse.ingest.api.model.compatibility;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.Dataset;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.SerializedSource;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.Source;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.format.MessageFormat;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.privacy.DataPrivacyClassification;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.preprocessing.Preprocessor;
import org.finos.legend.engine.protocol.pure.m3.relation.RelationType;

import java.io.IOException;

import static org.finos.legend.engine.protocol.pure.v1.ProcessHelper.processMany;
import static org.finos.legend.engine.protocol.pure.v1.ProcessHelper.processOne;

public class DatasetCustomDeserializer extends JsonDeserializer<Dataset>
{
    @Override
    public Dataset deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException
    {
        ObjectCodec codec = jsonParser.getCodec();
        JsonNode node = codec.readTree(jsonParser);

        Dataset dataset = new Dataset();
        dataset.name = processOne(node, "name", String.class, codec);
        dataset.primaryKey = processMany(node, "primaryKey", String.class, codec);
        dataset.ingestPartitionColumns = processMany(node, "ingestPartitionColumns", String.class, codec);
        dataset.storageLayoutClusterColumns = processMany(node, "storageLayoutClusterColumns", String.class, codec);
        dataset.storageLayoutPartitionColumns = processMany(node, "storageLayoutPartitionColumns", String.class, codec);
        dataset.privacyClassification = processOne(node, "privacyClassification", DataPrivacyClassification.class, codec);
        dataset.preprocessors = processMany(node, "preprocessors", Preprocessor.class, codec);
        if (node.get("source") != null)
        {
            dataset.source = processOne(node, "source", Source.class, codec);
        }
        else
        {
            SerializedSource source = new SerializedSource();
            if (node.get("schema") != null)
            {
                source.schema = processOne(node, "schema", RelationType.class, codec);
            }
            if (node.get("formatOverride") != null)
            {
                source.format = processOne(node, "formatOverride", MessageFormat.class, codec);
            }
            dataset.source = source;
        }
        return dataset;
    }
}
