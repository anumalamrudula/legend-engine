package com.gs.alloy.lakehouse.ingest.api.model;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.gs.alloy.lakehouse.ingest.api.model.compatibility.IngestBackwardCompatibility;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.IngestDefinition;
import org.eclipse.collections.api.block.function.Function0;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.tuple.Pair;
import org.finos.legend.engine.protocol.pure.v1.extension.ProtocolSubTypeInfo;
import org.finos.legend.engine.protocol.pure.v1.extension.PureProtocolExtension;
import org.finos.legend.engine.protocol.pure.m3.PackageableElement;

import java.util.List;
import java.util.Map;

public class IngestProtocolExtension implements PureProtocolExtension
{
    public static final String CLASSIFIER_PATH = "meta::external::ingest::specification::metamodel::IngestDefinition";
    public static final String NAME = "ingestDefinition";

    @Override
    public List<Function0<List<ProtocolSubTypeInfo<?>>>> getExtraProtocolSubTypeInfoCollectors()
    {
        return Lists.fixedSize.with(() -> Lists.fixedSize.with(
                ProtocolSubTypeInfo.newBuilder(PackageableElement.class)
                        .withSubtype(IngestDefinition.class, NAME)
                        .build()
        ));
    }

    @Override
    public Map<Class<? extends PackageableElement>, String> getExtraProtocolToClassifierPathMap()
    {
        return Maps.mutable.with(IngestDefinition.class, CLASSIFIER_PATH);
    }

    @Override
    public MutableList<Pair<Class, JsonDeserializer>> getExtraDeserializer()
    {
        return new IngestBackwardCompatibility().getExtraDeserializer();
    }
}
