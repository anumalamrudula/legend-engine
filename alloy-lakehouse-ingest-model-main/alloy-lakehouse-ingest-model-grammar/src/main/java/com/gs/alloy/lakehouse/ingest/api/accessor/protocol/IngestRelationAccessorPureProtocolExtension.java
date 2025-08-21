package com.gs.alloy.lakehouse.ingest.api.accessor.protocol;

import org.eclipse.collections.api.factory.Maps;
import org.finos.legend.engine.protocol.pure.v1.extension.PureProtocolExtension;

import java.util.Map;

public class IngestRelationAccessorPureProtocolExtension implements PureProtocolExtension
{
    @Override
    public Map<String, Class> getExtraClassInstanceTypeMappings()
    {
        return Maps.mutable.with("I", IngestRelationAccessor.class);
    }
}
