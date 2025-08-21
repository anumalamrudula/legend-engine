package com.gs.alloy.lakehouse.ingest.api.model.grammar;

import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.utility.LazyIterate;

import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;

public class SourcePluginLoader
{
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SourcePlugin.class);
    private static final AtomicReference<List<SourcePlugin>> INSTANCE = new AtomicReference<>();

    public static void logExtensionList()
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug(LazyIterate.collect(extensions(), extension -> "- " + extension.getClass().getSimpleName()).makeString("SourceLoader extension(s) loaded:\n", "\n", ""));
        }
    }

    public static List<SourcePlugin> extensions()
    {
        return INSTANCE.updateAndGet(existing ->
        {
            if (existing == null)
            {
                List<SourcePlugin> extensions = Lists.mutable.empty();
                for (SourcePlugin extension : ServiceLoader.load(SourcePlugin.class))
                {
                    try
                    {
                        extensions.add(extension);
                    }
                    catch (Throwable throwable)
                    {
                        LOGGER.error("Failed to load execution extension '" + extension.getClass().getSimpleName() + "'");
                        // Needs to be silent ... during the build process
                    }
                }
                return extensions;
            }
            return existing;
        });
    }

}
