package com.gs.alloy.lakehouse.ingest.api.model;

import org.finos.legend.pure.m3.serialization.filesystem.repository.CodeRepository;
import org.finos.legend.pure.m3.serialization.filesystem.repository.CodeRepositoryProvider;
import org.finos.legend.pure.m3.serialization.filesystem.repository.GenericCodeRepository;

public class IngestLakehouseCodeRepositoryProvider implements CodeRepositoryProvider
{
    @Override
    public CodeRepository repository()
    {
        return GenericCodeRepository.build("core_ingest.definition.json");
    }
}
