package com.gs.alloy.lakehouse.ingest.api.accessor.protocol;

import org.finos.legend.engine.protocol.m3.relation.RelationElementAccessor;
import org.finos.legend.engine.protocol.pure.m3.SourceInformation;

import java.util.List;

public class IngestRelationAccessor extends RelationElementAccessor
{
    public SourceInformation sourceInformation;

    public List<String> path;

    public Boolean metadata;
}
