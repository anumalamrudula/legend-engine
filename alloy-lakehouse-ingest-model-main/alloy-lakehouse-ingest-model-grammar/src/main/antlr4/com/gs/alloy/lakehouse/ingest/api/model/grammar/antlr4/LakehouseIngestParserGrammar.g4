parser grammar LakehouseIngestParserGrammar;

import M3ParserGrammar;

options
{
    tokenVocab = LakehouseIngestLexerGrammar;
}

// -------------------------------------- DEFINITION -------------------------------------

definition:                 (ingest)*
                            EOF
;

ingest:
    INGEST stereotypes? taggedValues? qualifiedName writeMode readMode ownerBackComp (GROUP EQUAL group=VALID_STRING)?
    BRACKET_OPEN
        (dataSet SEMI_COLON)+
    BRACKET_CLOSE
;

stereotype: qualifiedName DOT identifier
;

stereotypes: LESS_THAN LESS_THAN stereotype (COMMA stereotype)* GREATER_THAN GREATER_THAN
;

taggedValues:                                   BRACE_OPEN taggedValue (COMMA taggedValue)* BRACE_CLOSE
;
taggedValue:                                    qualifiedName DOT identifier EQUAL STRING
;

ownerBackComp : deploymentId | owner
;

owner : (OWNER EQUAL APPDIR PAREN_OPEN keyValues? PAREN_CLOSE)
;

keyValues: keyValue (COMMA keyValue)*
;

keyValue: VALID_STRING EQUAL STRING
;

deploymentId: (DEPLOYMENT_ID EQUAL id=INTEGER)
;

readMode: (snapshot | delta | undefined) (LESS_THAN format GREATER_THAN)?
;

snapshot : SNAPSHOT
;

delta : DELTA (PAREN_OPEN deleteColumn COMMA BRACKET_OPEN deleteValue (COMMA deleteValue)* BRACKET_CLOSE PAREN_CLOSE)?
;

undefined : UNDEFINED
;

deleteColumn : VALID_STRING
;

deleteValue : VALID_STRING
;

writeMode: APPEND_ONLY | BATCH_MILESTONED | OVERWRITE
;

dataSet: name (relationType | COLON anyLambda) sensitivity optionals*
;

name: VALID_STRING
;

sensitivity : DP00 | DP10 | DP20 | DP30
;

format : formatType genericParameters?
;

formatType : AVRO | CSV | JSON | PARQUET
;

genericParameters : BRACE_OPEN genericNameValue? (COMMA genericNameValue)* BRACE_CLOSE
;

genericNameValue : VALID_STRING EQUAL instanceLiteral
;

// Optionals

optionals : pk | partition | storageLayoutCluster | storageLayoutPartition | formatOverride | preprocessors
;

pk : PK EQUAL BRACKET_OPEN VALID_STRING (COMMA VALID_STRING)* BRACKET_CLOSE
;

partition : PARTITION EQUAL BRACKET_OPEN VALID_STRING (COMMA VALID_STRING)* BRACKET_CLOSE
;

storageLayoutCluster : STORAGE_LAYOUT_CLUSTER EQUAL BRACKET_OPEN VALID_STRING (COMMA VALID_STRING)* BRACKET_CLOSE
;

storageLayoutPartition : STORAGE_LAYOUT_PARTITION EQUAL BRACKET_OPEN VALID_STRING (COMMA VALID_STRING)* BRACKET_CLOSE
;

formatOverride : FORMAT EQUAL format
;

preprocessors : PREPROCESSORS EQUAL BRACKET_OPEN preprocessor (COMMA preprocessor)* BRACKET_CLOSE
;

preprocessor : processor = (FILTER_OUT_EXACT_DUPLICATE_RECORDS | TAKE_MAX_VERSION_FIELD_RECORD | INCLUDE_INZ_OUTZ_TIMESTAMP | OVERWRITE_ON_SNAPSHOT ) genericParameters?
;
