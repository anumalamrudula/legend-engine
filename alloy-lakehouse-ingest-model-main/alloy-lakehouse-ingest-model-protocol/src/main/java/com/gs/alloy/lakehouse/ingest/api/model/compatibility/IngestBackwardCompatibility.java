package com.gs.alloy.lakehouse.ingest.api.model.compatibility;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.IngestDefinition;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.Dataset;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

public class IngestBackwardCompatibility
{
    public void register(ObjectMapper objectMapper)
    {
        SimpleModule simpleModule = new SimpleModule();
        getExtraDeserializer().forEach(x -> simpleModule.addDeserializer(x.getOne(), x.getTwo()));
        objectMapper.registerModule(simpleModule);
    }

    public <T> MutableList<Pair<Class, JsonDeserializer>> getExtraDeserializer()
    {
        return Lists.mutable.with(
                Tuples.pair(IngestDefinition.class, new IngestDefinitionCustomDeserializer()),
                Tuples.pair(Dataset.class, new DatasetCustomDeserializer())
        );
    }
}
