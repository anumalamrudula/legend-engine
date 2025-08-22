package com.gs.alloy.lakehouse.ingest.api.model.temporary;

import com.gs.alloy.lakehouse.ingest.api.model.environment.infra.IngestEnvironmentClassification;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.AppDir;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.AppDirNode;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.Owner;

public class IngestProtocolOwnerHelper
{
    // This code is meant to support Logic at protocol level
    // However the code should migrate to the Compiled part (that contains this logic as polymorphic dispatch
    public static Long getId(Owner owner, Environment environment)
    {
        return getAppDirNode(owner, environment).appDirId;
    }

    public static AppDirNode getAppDirNode(Owner owner, Environment environment)
    {
        if (owner instanceof AppDir)
        {
            return environment.equals(Environment.Production) ? ((AppDir) owner).production : ((AppDir) owner).prodParallel == null ? ((AppDir) owner).production : ((AppDir) owner).prodParallel;
        }
        throw new RuntimeException("Not supported yet");
    }

    public static Long getId(Owner owner, IngestEnvironmentClassification environment)
    {
        return getId(owner, IngestEnvironmentClassification.PROD.equals(environment) ? Environment.Production : Environment.ProdParallel);
    }

    public static AppDirNode getAppDirNode(Owner owner, IngestEnvironmentClassification environment)
    {
        return getAppDirNode(owner, IngestEnvironmentClassification.PROD.equals(environment) ? Environment.Production : Environment.ProdParallel);
    }
}
