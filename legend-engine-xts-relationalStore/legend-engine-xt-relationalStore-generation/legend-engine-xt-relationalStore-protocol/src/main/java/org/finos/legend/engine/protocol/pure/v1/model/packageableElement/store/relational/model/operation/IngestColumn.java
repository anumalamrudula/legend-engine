package org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation;

import org.finos.legend.engine.protocol.pure.v1.model.context.PackageableElementPointer;
import java.util.List;

public class IngestColumn extends RelationalOperationElement
{
    public PackageableElementPointer ingest;
    public List<String> path; // e.g. [DataTypes, TINY_INT_VAL]
}