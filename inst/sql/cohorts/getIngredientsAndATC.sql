select distinct concept_id, concept_name, vocabulary_id,
  CASE WHEN vocabulary_id = 'ATC' THEN 1 else 0 END AS ATC_flg
from @vocabulary_database_schema.concept
where (concept_class_id = 'Ingredient' AND vocabulary_id = 'RxNorm' AND standard_concept = 'S')
   OR (concept_class_id = 'ATC 4th' AND vocabulary_id = 'ATC')
