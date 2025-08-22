package com.gs.alloy.lakehouse.ingest.api.model.compatibility;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.IngestDefinition;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.WriteMode;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.Dataset;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.AppDir;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.AppDirLevel;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.AppDirNode;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.Owner;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.readMode.ReadMode;
import org.finos.legend.engine.protocol.pure.m3.SourceInformation;
import org.finos.legend.engine.protocol.pure.m3.extension.StereotypePtr;
import org.finos.legend.engine.protocol.pure.m3.extension.TaggedValue;

import java.io.IOException;

import static org.finos.legend.engine.protocol.pure.v1.ProcessHelper.processMany;
import static org.finos.legend.engine.protocol.pure.v1.ProcessHelper.processOne;

public class IngestDefinitionCustomDeserializer extends JsonDeserializer<IngestDefinition>
{
    @Override
    public IngestDefinition deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException
    {
        ObjectCodec codec = jsonParser.getCodec();
        JsonNode node = codec.readTree(jsonParser);

        IngestDefinition ingestDefinition = new IngestDefinition();
        ingestDefinition.name = processOne(node, "name", String.class, codec);
        ingestDefinition.stereotypes = processMany(node, "stereotypes", StereotypePtr.class, codec);
        ingestDefinition.taggedValues = processMany(node, "taggedValues", TaggedValue.class, codec);
        ingestDefinition._package = processOne(node, "package", String.class, codec);
        ingestDefinition.sourceInformation = processOne(node, "sourceInformation", SourceInformation.class, codec);

        ingestDefinition.readMode = processOne(node, "readMode", ReadMode.class, codec);
        ingestDefinition.writeMode = processOne(node, "writeMode", WriteMode.class, codec);
        ingestDefinition.datasets = processMany(node, "datasets", Dataset.class, codec);
        ingestDefinition.datasetGroup = processOne(node, "datasetGroup", String.class, codec);

        if (node.get("owner") != null)
        {
            ingestDefinition.owner = processOne(node, "owner", Owner.class, codec);
        }
        else
        {
            OldAppDirDeployment old = processOne(node, "appDirDeployment", OldAppDirDeployment.class, codec);
            ingestDefinition.owner = new AppDir()
                        //._prodParallel(new AppDirNode()._appDirId(old.appDirId)._level(AppDirLevel.valueOf(old.level)))
                        ._production(new AppDirNode()._appDirId(old.appDirId)._level(AppDirLevel.valueOf(old.level))
                    );
        }

        ingestDefinition.batchAttributesTaxonomyType = processOne(node, "batchAttributesTaxonomyType", String.class, codec);

        return ingestDefinition;
    }

    private static class OldAppDirDeployment
    {
        public Long appDirId;
        public String level;
    }
}
