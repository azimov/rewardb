DELETE FROM @cohort_database_schema.@concept_set_definition_table;
INSERT INTO @cohort_database_schema.@concept_set_definition_table (
    CONCEPT_ID,
    CONCEPTSET_NAME,
    CONCEPTSET_ID,
    ISEXCLUDED,
    INCLUDEDESCENDANTS,
    INCLUDEMAPPED
)
SELECT DISTINCT concept_id , concept_name, concept_id, 0, 1, 0
    from @vocabulary_database_schema.concept
    where (concept_class_id = 'Ingredient' AND vocabulary_id = 'RxNorm' AND standard_concept = 'S')
       OR (concept_class_id = 'ATC 4th' AND vocabulary_id = 'ATC');

DELETE FROM @cohort_database_schema.@cohort_definition_table;
INSERT INTO @cohort_database_schema.@cohort_definition_table (
    COHORT_DEFINITION_ID,
    COHORT_DEFINITION_NAME,
    SHORT_NAME,
    DRUG_CONCEPTSET_ID,
    INDICATION_CONCEPTSET_ID,
    TARGET_COHORT,
    SUBGROUP_COHORT,
    ATC_FLG
)
SELECT DISTINCT
    CAST(CONCEPT_ID AS BIGINT) * 1000,
    CONCEPT_NAME,
    CONCAT(VOCABULARY_ID, ' - ', CONCEPT_NAME),
    CONCEPT_ID,
    0,
    0,
    0,
    CASE WHEN vocabulary_id = 'ATC' THEN 1 else 0 END AS ATC_FLAG
FROM @vocabulary_database_schema.concept
WHERE (concept_class_id = 'Ingredient' AND vocabulary_id = 'RxNorm' AND standard_concept = 'S')
OR (concept_class_id = 'ATC 4th' AND vocabulary_id = 'ATC');