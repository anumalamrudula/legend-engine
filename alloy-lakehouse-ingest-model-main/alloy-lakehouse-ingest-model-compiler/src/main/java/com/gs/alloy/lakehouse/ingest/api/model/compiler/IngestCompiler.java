package com.gs.alloy.lakehouse.ingest.api.model.compiler;

import com.gs.alloy.lakehouse.common.precise.primitives.subtypes.ExtractPrecisePrimitiveFromSubTypes;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.IngestDefinition;
import com.gs.alloy.lakehouse.ingest.api.model.ValidationError;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.Dataset;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.SerializedSource;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.Source;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.format.*;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.dataset.privacy.DataPrivacyClassification;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.AppDir;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.AppDirNode;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.organization.Owner;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.preprocessing.Preprocessor;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.preprocessing.augmentation.IncludeInZOutZTimestamp;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.preprocessing.deduplication.FilterOutExactDuplicateRecords;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.preprocessing.deduplication.OverwriteOnSnapshot;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.preprocessing.deduplication.TakeMaxVersionFieldRecord;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.preprocessing.versioning.*;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.readMode.ReadMode;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.readMode.Undefined;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.readMode.delta.DeleteIndicator;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.readMode.delta.Delta;
import com.gs.alloy.lakehouse.ingest.api.model.specification.protocol.readMode.snapshot.Snapshot;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.*;
import org.finos.legend.engine.protocol.pure.m3.SourceInformation;
import org.finos.legend.engine.protocol.pure.m3.relation.Column;
import org.finos.legend.engine.protocol.pure.m3.valuespecification.constant.PackageableType;
import org.finos.legend.engine.protocol.pure.v1.model.context.EngineErrorType;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.shared.core.deployment.DeploymentMode;
import org.finos.legend.engine.shared.core.operational.errorManagement.EngineException;
import org.finos.legend.pure.generated.*;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.relation.RelationType;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.type.PrimitiveType;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.type.TypeAccessor;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.type.generics.GenericType;
import org.finos.legend.pure.m3.navigation.PackageableElement.PackageableElement;
import org.finos.legend.pure.m3.navigation.relation._Column;
import org.finos.legend.pure.m3.navigation.relation._RelationType;

import java.util.List;

public class IngestCompiler
{
    private static final String PRECISE_PRIMITIVE_PURE_PACKAGE = "meta::pure::precisePrimitives";
    private static final String VARIANT_PACKAGE = "meta::pure::metamodel::variant";


    // Simple entry point
    public Root_meta_external_ingest_specification_metamodel_IngestDefinition compile(IngestDefinition ingestDefinition)
    {
        return this.compileFirst(
                ingestDefinition,
                new CompileContext.Builder(new PureModel(new PureModelContextData.Builder().build(), null, DeploymentMode.PROD))
                        .withImports(Sets.immutable.with(PRECISE_PRIMITIVE_PURE_PACKAGE, VARIANT_PACKAGE))
                        .build()
        );
    }

    // Global compiler entry point
    public Root_meta_external_ingest_specification_metamodel_IngestDefinition compileFirst(IngestDefinition ingestDefinition, CompileContext _context)
    {
        CompileContext context = new CompileContext.Builder(_context).withImports(Sets.immutable.with(PRECISE_PRIMITIVE_PURE_PACKAGE, VARIANT_PACKAGE)).build();

        List<ValidationError> res = ingestDefinition.validate();
        if (!res.isEmpty())
        {
            throw new RuntimeException(Lists.mutable.withAll(res).collect(c -> c.constraintId + ": " + c.message).makeString("\n"));
        }

        return new Root_meta_external_ingest_specification_metamodel_IngestDefinition_Impl(ingestDefinition.name, null, context.pureModel.getClass("meta::external::ingest::specification::metamodel::IngestDefinition"))
                ._name(ingestDefinition.name)
                ._readMode(compileFirst(ingestDefinition.readMode, context))
                ._writeMode(context.pureModel.getEnumValue("meta::external::ingest::specification::metamodel::WriteMode", ingestDefinition.writeMode.getName()))
                ._datasets(ListIterate.collect(ingestDefinition.datasets, x -> compileFirst(x, context)))
                ._datasetGroup(ingestDefinition.datasetGroup)
                ._owner(compileFirst(ingestDefinition.owner, context))
                ._batchAttributesTaxonomyType(ingestDefinition.batchAttributesTaxonomyType == null ? null : context.pureModel.getClass(ingestDefinition.batchAttributesTaxonomyType))
                ._stereotypes(ingestDefinition.stereotypes == null ? Lists.fixedSize.empty() : ListIterate.collect(ingestDefinition.stereotypes, stereotypePointer -> context.resolveStereotype(stereotypePointer.profile, stereotypePointer.value, stereotypePointer.profileSourceInformation, stereotypePointer.sourceInformation)))
                ._taggedValues(ingestDefinition.taggedValues == null ? Lists.fixedSize.empty() : ListIterate.collect(ingestDefinition.taggedValues, taggedValue -> new Root_meta_pure_metamodel_extension_TaggedValue_Impl("", null, context.pureModel.getClass("meta::pure::metamodel::extension::TaggedValue"))._tag(context.resolveTag(taggedValue.tag.profile, taggedValue.tag.value, taggedValue.tag.profileSourceInformation, taggedValue.tag.sourceInformation))._value(taggedValue.value)));
    }

    private Root_meta_external_ingest_specification_metamodel_organization_Owner compileFirst(Owner deployment, CompileContext _context)
    {
        AppDir appDir = (AppDir) deployment;
        return new Root_meta_external_ingest_specification_metamodel_organization_AppDir_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::organization::AppDir"))
                ._prodParallel(compileFirst(appDir.prodParallel, _context))
                ._production(compileFirst(appDir.production, _context));
    }

    private Root_meta_external_ingest_specification_metamodel_organization_AppDirNode compileFirst(AppDirNode appDirNode, CompileContext _context)
    {
        return appDirNode == null ? null : new Root_meta_external_ingest_specification_metamodel_organization_AppDirNode_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::organization::AppDirNode"))
                ._appDirId(appDirNode.appDirId)
                ._level(_context.pureModel.getEnumValue("meta::external::ingest::specification::metamodel::organization::AppDirLevel", appDirNode.level.getName()));
    }

    private Root_meta_external_ingest_specification_metamodel_dataset_Dataset compileFirst(Dataset d, CompileContext _context)
    {
        Root_meta_external_ingest_specification_metamodel_dataset_Dataset res = new Root_meta_external_ingest_specification_metamodel_dataset_Dataset_Impl(d.name, null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::dataset::Dataset"))
                ._name(d.name)
                ._source(compileFirst(d.source, _context))
                ._privacyClassification(compileFirst(d.privacyClassification, _context))
                ._preprocessors(ListIterate.collect(d.preprocessors, p -> compileFirst(p, _context)));

        if (d.source instanceof SerializedSource)
        {
            RelationType<?> relationType = ((Root_meta_external_ingest_specification_metamodel_dataset_SerializedSource) res._source())._schema();
            return res
                    ._primaryKey(validateAndExtractPK(d.primaryKey, relationType))
                    ._ingestPartitionColumns(ListIterate.collect(d.ingestPartitionColumns, p -> relationType._columns().detect(x -> x._name().equals(p))))
                    ._storageLayoutClusterColumns(ListIterate.collect(d.storageLayoutClusterColumns, p -> relationType._columns().detect(x -> x._name().equals(p))))
                    ._storageLayoutPartitionColumns(ListIterate.collect(d.storageLayoutPartitionColumns, p -> relationType._columns().detect(x -> x._name().equals(p))));
        }
        return res;
    }

    private Root_meta_external_ingest_specification_metamodel_dataset_Source compileFirst(Source source, CompileContext _context)
    {
        if (source instanceof SerializedSource)
        {

            org.finos.legend.engine.protocol.pure.m3.relation.RelationType s = remapGenericTypeFromPrecisePrimitives(((SerializedSource) source).schema);
            RelationType<?> schema = RelationTypeHelper.convert(s, _context);
            validate(schema, _context, s);
            return new Root_meta_external_ingest_specification_metamodel_dataset_SerializedSource_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::dataset::SerializedSource"))
                    ._schema(schema)
                    ._format(compileFirst(((SerializedSource) source).format, _context));
        }
        return null;
    }

    private void validate(RelationType<?> relationType, CompileContext _context, org.finos.legend.engine.protocol.pure.m3.relation.RelationType s)
    {
        relationType._columns().forEach(c ->
        {
            GenericType columnType = _Column.getColumnType(c);
            String typePath = PackageableElement.getUserPathForPackageableElement(columnType._rawType());

            if (!typePath.startsWith(PRECISE_PRIMITIVE_PURE_PACKAGE)
                    && !typePath.startsWith(VARIANT_PACKAGE)
                    && !typePath.equals("StrictDate"))
            {
                if (ExtractPrecisePrimitiveFromSubTypes.typeExtendsPrecisePrimitive(columnType))
                {
                    typePath = ((PackageableType) ExtractPrecisePrimitiveFromSubTypes.getPrecisePrimitiveExtendedBySubtype(typePath).rawType).fullPath;
                }
            }

            if (!typePath.startsWith(PRECISE_PRIMITIVE_PURE_PACKAGE)
                    && !typePath.startsWith(VARIANT_PACKAGE)
                    && !typePath.equals("StrictDate")
                    && !typePath.equals("StrictTime")
                    && !typePath.equals("Boolean")
            )
            {
                Column column = s != null ? s.columns.stream().filter(col -> col.name.equals(c._name())).findFirst().orElse(null) : null;
                SourceInformation sourceInformation = column != null && column.genericType.rawType instanceof PackageableType ? ((PackageableType) column.genericType.rawType).sourceInformation : null;
                throw new EngineException("Error validating the relation " + _RelationType.print(relationType, _context.pureModel.getExecutionSupport().getProcessorSupport()) + ". The type '" + typePath + "' is not authorized in Lakehouse. The following types are authorized: " +
                        _context.resolvePackageableElement(PRECISE_PRIMITIVE_PURE_PACKAGE + "::Int")
                                ._package()._children().selectInstancesOf(PrimitiveType.class).collect(TypeAccessor::_name).toSortedList().makeString(", ") + ", StrictTime" + ", StrictDate" + ", Variant" + ", Boolean", sourceInformation, EngineErrorType.COMPILATION);
            }
        });
    }

    private Root_meta_external_ingest_specification_metamodel_readMode_ReadMode compileFirst(ReadMode readMode, CompileContext _context)
    {
        if (readMode instanceof Delta)
        {
            return new Root_meta_external_ingest_specification_metamodel_readMode_delta_Delta_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::readMode::delta::Delta"))
                    ._format(compileFirst(readMode.format, _context))
                    ._deleteIndicator(compileFirst(((Delta) readMode).deleteIndicator, _context));
        }
        else if (readMode instanceof Snapshot)
        {
            return new Root_meta_external_ingest_specification_metamodel_readMode_snapshot_Snapshot_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::readMode::delta::Delta"))
                    ._format(compileFirst(readMode.format, _context));
        }
        else if (readMode instanceof Undefined)
        {
            return new Root_meta_external_ingest_specification_metamodel_readMode_Undefined_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::readMode::delta::Delta"))
                    ._format(compileFirst(readMode.format, _context));

        }
        throw new RuntimeException(readMode.getClass() + " is not supported yet!");
    }

    private Root_meta_external_ingest_specification_metamodel_readMode_delta_DeleteIndicator compileFirst(DeleteIndicator deleteIndicator, CompileContext _context)
    {
        return deleteIndicator == null ? null : new Root_meta_external_ingest_specification_metamodel_readMode_delta_DeleteIndicator_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::readMode::delta::DeleteIndicator"))
                ._deleteField(deleteIndicator.deleteField)
                ._deleteValues(Lists.mutable.withAll(deleteIndicator.deleteValues));
    }

    private Root_meta_external_ingest_specification_metamodel_dataset_format_MessageFormat compileFirst(MessageFormat format, CompileContext _context)
    {
        if (format == null)
        {
            return null;
        }
        else if (format instanceof CsvMessageFormat)
        {
            CsvMessageFormat csv = (CsvMessageFormat) format;
            return new Root_meta_external_ingest_specification_metamodel_dataset_format_CsvMessageFormat_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::dataset::format::CsvMessageFormat"))
                    ._fieldDelimiter(csv.fieldDelimiter)
                    ._headerRowsToSkipCount(csv.headerRowsToSkipCount)
                    ._quoteCharacter(csv.quoteCharacter)
                    ._escapeCharacter(csv.escapeCharacter)
                    ._recordDelimiter(csv.recordDelimiter);
        }
        else if (format instanceof AvroMessageFormat)
        {
            return new Root_meta_external_ingest_specification_metamodel_dataset_format_AvroMessageFormat_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::dataset::format::AvroMessageFormat"));
        }
        else if (format instanceof JsonMessageFormat)
        {
            return new Root_meta_external_ingest_specification_metamodel_dataset_format_JsonMessageFormat_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::dataset::format::JsonMessageFormat"));
        }
        else if (format instanceof ParquetMessageFormat)
        {
            return new Root_meta_external_ingest_specification_metamodel_dataset_format_ParquetMessageFormat_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::dataset::format::ParquetMessageFormat"));
        }
        throw new RuntimeException(format.getClass() + " is not supported yet!");
    }

    private Root_meta_external_ingest_specification_metamodel_dataset_privacy_DataPrivacyClassification compileFirst(DataPrivacyClassification privacyClassification, CompileContext _context)
    {
        return new Root_meta_external_ingest_specification_metamodel_dataset_privacy_DataPrivacyClassification_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::dataset::format::ParquetMessageFormat"))
                ._sensitivity(_context.pureModel.getEnumValue("meta::external::ingest::specification::metamodel::dataset::privacy::DataSensitivity", privacyClassification.sensitivity.getName()))
                ._type(privacyClassification.type == null ? null : _context.pureModel.getEnumValue("meta::external::ingest::specification::metamodel::dataset::privacy::DataType", privacyClassification.type.getName()));
    }

    private Root_meta_external_ingest_specification_metamodel_preprocessing_Preprocessor compileFirst(Preprocessor p, CompileContext _context)
    {
        if (p instanceof TakeMaxVersionFieldRecord)
        {
            return new Root_meta_external_ingest_specification_metamodel_preprocessing_deduplication_TakeMaxVersionFieldRecord_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::preprocessing::deduplication::TakeMaxVersionFieldRecord"))
                    ._versionField(((TakeMaxVersionFieldRecord) p).versionField)._requireIncreasingVersion(((TakeMaxVersionFieldRecord) p).requireIncreasingVersion);
        }
        else if (p instanceof FilterOutExactDuplicateRecords)
        {
            return new Root_meta_external_ingest_specification_metamodel_preprocessing_deduplication_FilterOutExactDuplicateRecords_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::preprocessing::deduplication::FilterOutExactDuplicateRecords"));
        }
        else if (p instanceof IncludeInZOutZTimestamp)
        {
            return new Root_meta_external_ingest_specification_metamodel_preprocessing_augmentation_IncludeInZOutZTimestamp_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::preprocessing::augmentation::IncludeInZOutZTimestamp"));
        }
        else if (p instanceof OverwriteOnSnapshot)
        {
            return new Root_meta_external_ingest_specification_metamodel_preprocessing_deduplication_OverwriteOnSnapshot_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::preprocessing::deduplication::OverwriteOnSnapshot"));
        }
        else if (p instanceof FieldBasedVersioning)
        {
            FieldBasedVersioning fieldBasedVersioning = (FieldBasedVersioning) p;
            return new Root_meta_external_ingest_specification_metamodel_preprocessing_versioning_FieldBasedVersioning_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::preprocessing::versioning::FieldBasedVersioning"))
                    ._versionField(fieldBasedVersioning.versionField)
                    ._multipleVersionHandling(compileFirst(fieldBasedVersioning.multipleVersionHandling, _context))
                    ._updateHandling(compileFirst(fieldBasedVersioning.updateHandling, _context));
        }
        throw new RuntimeException(p.getClass() + " is not supported yet!");
    }

    private Root_meta_external_ingest_specification_metamodel_preprocessing_versioning_UpdateHandling compileFirst(UpdateHandling updateHandling, CompileContext _context)
    {
        if (updateHandling instanceof AlloyAnyVersionUpdateHandling)
        {
            return new Root_meta_external_ingest_specification_metamodel_preprocessing_versioning_AlloyAnyVersionUpdateHandling_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::preprocessing::versioning::AlloyAnyVersionUpdateHandling"));
        }
        else if (updateHandling instanceof RequireIncreasingVersionUpdateHandling)
        {
            return new Root_meta_external_ingest_specification_metamodel_preprocessing_versioning_RequireIncreasingVersionUpdateHandling_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::preprocessing::versioning::RequireIncreasingVersionUpdateHandling"));
        }
        throw new RuntimeException(updateHandling.getClass() + " is not supported yet!");
    }

    private Root_meta_external_ingest_specification_metamodel_preprocessing_versioning_MultipleVersionsHandling compileFirst(MultipleVersionsHandling multipleVersionHandling, CompileContext _context)
    {
        if (multipleVersionHandling instanceof TakeAll)
        {
            return new Root_meta_external_ingest_specification_metamodel_preprocessing_versioning_TakeAll_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::preprocessing::versioning::TakeAll"))
                    ._comparisonOperator(_context.pureModel.getEnumValue("meta::external::ingest::specification::metamodel::preprocessing::versioning::VersionComparisonOperator", multipleVersionHandling.comparisonOperator.getName()));
        }
        else if (multipleVersionHandling instanceof TakeMaxMultipleVersionsHandling)
        {
            return new Root_meta_external_ingest_specification_metamodel_preprocessing_versioning_TakeMaxMultipleVersionsHandling_Impl("", null, _context.pureModel.getClass("meta::external::ingest::specification::metamodel::preprocessing::versioning::TakeMaxMultipleVersionsHandling"))
                    ._comparisonOperator(_context.pureModel.getEnumValue("meta::external::ingest::specification::metamodel::preprocessing::versioning::VersionComparisonOperator", multipleVersionHandling.comparisonOperator.getName()));
        }
        throw new RuntimeException(multipleVersionHandling.getClass() + " is not supported yet!");
    }

    private static org.finos.legend.engine.protocol.pure.m3.relation.RelationType remapGenericTypeFromPrecisePrimitives(org.finos.legend.engine.protocol.pure.m3.relation.RelationType relationType)
    {
        return new org.finos.legend.engine.protocol.pure.m3.relation.RelationType(
                ListIterate.collect(relationType.columns, c ->
                        {
                            org.finos.legend.engine.protocol.pure.m3.type.generics.GenericType genericType;
                            if (c.genericType.rawType instanceof PackageableType && ((PackageableType) c.genericType.rawType).fullPath != null)
                            {
                                String fullPath = ((PackageableType) c.genericType.rawType).fullPath;
                                SourceInformation sourceInformation = ((PackageableType) c.genericType.rawType).sourceInformation;
                                if (fullPath.equals("Decimal"))
                                {
                                    genericType = rebuildGenericType(c.genericType, fullPath.replace("Decimal", "Numeric"), sourceInformation);
                                }
                                else if (fullPath.equals("Date"))
                                {
                                    genericType = rebuildGenericType(c.genericType, "StrictDate", sourceInformation);
                                }
                                else if (fullPath.equals("Time"))
                                {
                                    genericType = rebuildGenericType(c.genericType, "StrictTime", sourceInformation);
                                }
                                else
                                {
                                    genericType = c.genericType;
                                }
                            }
                            else
                            {
                                genericType = c.genericType;
                            }
                            return new Column(
                                    c.name,
                                    genericType,
                                    c.multiplicity);
                        }
                )
        );
    }

    private static org.finos.legend.engine.protocol.pure.m3.type.generics.GenericType rebuildGenericType(org.finos.legend.engine.protocol.pure.m3.type.generics.GenericType oldGenericType, String newType, SourceInformation sourceInformation)
    {
        PackageableType packageableType = new PackageableType(newType);
        if (sourceInformation != null)
        {
            sourceInformation.endColumn = sourceInformation.startColumn + newType.length() - 1;
        }
        packageableType.sourceInformation = sourceInformation;
        return new org.finos.legend.engine.protocol.pure.m3.type.generics.GenericType(
                packageableType,
                oldGenericType.typeVariableValues,
                oldGenericType.typeArguments,
                oldGenericType.multiplicityArguments
        );
    }

    public org.finos.legend.engine.protocol.pure.m3.relation.RelationType getSchema(Dataset dataset, CompileContext context)
    {
        if (dataset.source instanceof SerializedSource)
        {
            return ((SerializedSource) dataset.source).schema;
        }
        // This operation should be done using the compiled graph....
//        else if (dataset.source instanceof FunctionSource)
//        {
//            LambdaFunction lambda = ((FunctionSource) dataset.source).function;
//            org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.LambdaFunction<?> l = HelperValueSpecificationBuilder.buildLambdaWithContext(lambda.body, lambda.parameters, context, new ProcessingContext("Lambda compilation"));
//            RelationType<?> functionReturnRelationType = (RelationType<?>) ((FunctionType) Function.computeFunctionType(l, context.pureModel.getExecutionSupport().getProcessorSupport()))._returnType()._typeArguments().getFirst()._rawType();
//            return RelationTypeHelper.convert(functionReturnRelationType);
//        }
        throw new RuntimeException(dataset.source.getClass() + " not supported yet");
    }

    public void compileThird(IngestDefinition ingestDef, CompileContext _context)
    {
        final Root_meta_external_ingest_specification_metamodel_IngestDefinition ingestDefinition = (Root_meta_external_ingest_specification_metamodel_IngestDefinition) _context.pureModel.getPackageableElement(_context.pureModel.buildPackageString(ingestDef._package, ingestDef.name), ingestDef.sourceInformation);
        ingestDefinition._datasets().zip(ingestDef.datasets).forEach(ds ->
        {
            if (!(ds.getTwo().source instanceof SerializedSource))
            {
                ds.getOne()._source(compileThird(ds.getTwo().source, _context));
                RelationType<?> schema = ds.getOne()._source().relationType(_context.pureModel.getExecutionSupport());
                validate(schema, _context, null);
                ds.getOne()
                        ._primaryKey(validateAndExtractPK(ds.getTwo().primaryKey, schema))
                        ._ingestPartitionColumns(ListIterate.collect(ds.getTwo().ingestPartitionColumns, p -> schema._columns().detect(x -> x._name().equals(p))))
                        ._storageLayoutClusterColumns(ListIterate.collect(ds.getTwo().storageLayoutClusterColumns, p -> schema._columns().detect(x -> x._name().equals(p))))
                        ._storageLayoutPartitionColumns(ListIterate.collect(ds.getTwo().storageLayoutPartitionColumns, p -> schema._columns().detect(x -> x._name().equals(p))))
                ;
            }
        });

    }

    public RichIterable<org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.relation.Column<?, ?>> validateAndExtractPK(List<String> primaryKeys, RelationType<?> relationType)
    {
        return ListIterate.collect(primaryKeys, p ->
                {

                    org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.relation.Column<?, ?> col = relationType._columns().detect(x -> x._name().equals(p));
                    if (col == null)
                    {
                        throw new EngineException(String.format("No column called %s found in relation schema", p), null,  EngineErrorType.COMPILATION);
                    }
                    return col;
                }
        );
    }

    public Root_meta_external_ingest_specification_metamodel_dataset_Source compileThird(Source source, CompileContext _context)
    {
        for (SourceCompilerExtension sourceCompilerExtension : SourceCompilerExtensionLoader.extensions())
        {
            Root_meta_external_ingest_specification_metamodel_dataset_Source result = sourceCompilerExtension.compile(source, _context);
            if (result != null)
            {
                return result;
            }
        }
        throw new RuntimeException(source.getClass() + " is not supported yet!");
    }

}
