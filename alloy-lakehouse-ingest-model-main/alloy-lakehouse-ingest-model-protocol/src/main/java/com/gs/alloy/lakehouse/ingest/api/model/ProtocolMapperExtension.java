package com.gs.alloy.lakehouse.ingest.api.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.gs.alloy.lakehouse.common.object.mapper.MapperExtension;
import com.gs.alloy.lakehouse.ingest.api.model.compatibility.IngestBackwardCompatibility;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.IngestDefinition;

public class ProtocolMapperExtension implements MapperExtension
{
    @Override
    public ObjectMapper extend(ObjectMapper objectMapper)
    {
        objectMapper.registerSubtypes(new NamedType(IngestDefinition.class, IngestProtocolExtension.NAME));
        new IngestBackwardCompatibility().register(objectMapper);
        return objectMapper;
    }
}
