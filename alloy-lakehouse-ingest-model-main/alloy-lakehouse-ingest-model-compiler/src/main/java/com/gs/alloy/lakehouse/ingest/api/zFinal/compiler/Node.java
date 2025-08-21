package com.gs.alloy.lakehouse.ingest.api.zFinal.compiler;

import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.MutableSet;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.PackageableElement;

public class Node
{
    private org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.PackageableElement element;
    private final MutableSet<Node> dependencies = Sets.mutable.empty();

    public Node(org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.PackageableElement element)
    {
        this.element = element;
    }

    public PackageableElement getElement()
    {
        return element;
    }

    public void addDependency(Node dependency)
    {
        dependencies.add(dependency);
    }

    public void removeDependency(Node dependency)
    {
        dependencies.remove(dependency);
    }

    public MutableSet<Node> getDependencies()
    {
        return dependencies;
    }
}
