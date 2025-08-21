lexer grammar LakehouseIngestLexerGrammar;

import M3LexerGrammar;

INGEST:                          'Ingest';

APPEND_ONLY:                     'AppendOnly';
BATCH_MILESTONED:                'BatchMilestoned';
OVERWRITE:                       'Overwrite';

DELTA:                           'Delta';
SNAPSHOT:                        'Snapshot';
UNDEFINED:                       'Undefined';

GROUP:                           'group';

DEPLOYMENT_ID:                   'deploymentId';
OWNER:                           'owner';
APPDIR:                          'AppDir';

DP00:                            'DP00';
DP10:                            'DP10';
DP20:                            'DP20';
DP30:                            'DP30';

AVRO:                            'AVRO';
CSV:                             'CSV';
JSON:                            'JSON';
PARQUET:                         'PARQUET';

PK:                              'pk';
PARTITION:                       'partition';
STORAGE_LAYOUT_PARTITION:        'storageLayoutPartition';
STORAGE_LAYOUT_CLUSTER:          'storageLayoutCluster';
FORMAT:                          'format';
PREPROCESSORS:                   'preprocessors';

FILTER_OUT_EXACT_DUPLICATE_RECORDS:  'FilterOutExactDuplicateRecords';
TAKE_MAX_VERSION_FIELD_RECORD:       'TakeMaxVersionFieldRecord';
INCLUDE_INZ_OUTZ_TIMESTAMP:          'IncludeInZOutZTimestamp';
OVERWRITE_ON_SNAPSHOT:               'OverwriteOnSnapshot';
