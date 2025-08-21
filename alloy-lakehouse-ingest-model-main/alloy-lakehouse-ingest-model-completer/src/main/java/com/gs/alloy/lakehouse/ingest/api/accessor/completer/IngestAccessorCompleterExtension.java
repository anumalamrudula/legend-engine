package com.gs.alloy.lakehouse.ingest.api.accessor.completer;

import com.gs.alloy.lakehouse.ingest.api.accessor.protocol.IngestRelationAccessor;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.repl.autocomplete.CompleterExtension;
import org.finos.legend.engine.repl.autocomplete.CompletionItem;
import org.finos.legend.engine.repl.autocomplete.CompletionResult;
import org.finos.legend.engine.repl.autocomplete.parser.ParserFixer;
import org.finos.legend.pure.generated.Root_meta_external_ingest_specification_metamodel_IngestDefinition;
import org.finos.legend.pure.generated.Root_meta_external_ingest_specification_metamodel_dataset_Dataset;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.PackageableElement;

public class IngestAccessorCompleterExtension implements CompleterExtension
{
    @Override
    public CompletionResult extraClassInstanceProcessor(Object islandExpr, PureModel pureModel)
    {
        if (islandExpr instanceof IngestRelationAccessor)
        {
            IngestRelationAccessor ingestRelationAccessor = (IngestRelationAccessor) islandExpr;
            MutableList<String> path = Lists.adapt(ingestRelationAccessor.path);

            if (path.anySatisfy(x -> x.isEmpty() || x.contains(ParserFixer.magicToken)))
            {
                String writtenPath = path.get(0).replace(ParserFixer.magicToken, "");
                MutableList<Root_meta_external_ingest_specification_metamodel_IngestDefinition> elements = pureModel.getAllPackageableElementsOfType(Root_meta_external_ingest_specification_metamodel_IngestDefinition.class).select(c -> nameMatch(c, writtenPath)).toList();

                MutableList<CompletionItem> completionItems = Lists.mutable.empty();

                if (elements.size() == 1 && path.size() > 1)
                {
                    String dsName = path.get(1).replace(ParserFixer.magicToken, "").replace("::", "");
                    completionItems.addAll(getDatasetSuggestions(elements.get(0), dsName));
                }
                else
                {
                    ListIterate.collect(elements, c -> new CompletionItem(org.finos.legend.pure.m3.navigation.PackageableElement.PackageableElement.getUserPathForPackageableElement(c), "I{" + org.finos.legend.pure.m3.navigation.PackageableElement.PackageableElement.getUserPathForPackageableElement(c) + '.'), completionItems);
                }
                return new CompletionResult(completionItems);
            }
        }
        return null;
    }

    private MutableList<CompletionItem> getDatasetSuggestions(Root_meta_external_ingest_specification_metamodel_IngestDefinition ingestDefinition, String dsName)
    {
        return ingestDefinition._datasets().select(d -> d._name().startsWith(dsName)).toSortedListBy(Root_meta_external_ingest_specification_metamodel_dataset_Dataset::_name).collect(x -> new CompletionItem(x._name(), x._name() + "}#"));
    }

    private static boolean nameMatch(PackageableElement c, String writtenPath)
    {
        String path = org.finos.legend.pure.m3.navigation.PackageableElement.PackageableElement.getUserPathForPackageableElement(c);
        if (path.length() > writtenPath.length())
        {
            return path.startsWith(writtenPath);
        }
        else
        {
            return writtenPath.startsWith(path);
        }
    }
}
