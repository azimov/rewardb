
-- FINDING DRUGS INDICATED FOR GIVEN CONDITIONS
select distinct
  c.concept_id as c_id, c.concept_name as c_name, c.vocabulary_id as c_vocab, c.domain_id as c_domain, c.concept_class_id as c_class, -- Condition
  de.concept_id as de_id, de.concept_name as de_name, de.vocabulary_id as de_vocab, de.domain_id as de_domain, de.concept_class_id as de_class -- Drug
from VOCABULARY_20200910.dbo.concept an -- Indications
join VOCABULARY_20200910.dbo.concept_ancestor a on a.ancestor_concept_id=an.concept_id -- connect to concept table
join VOCABULARY_20200910.dbo.concept de on de.concept_id=a.descendant_concept_id -- Drugs
join VOCABULARY_20200910.dbo.concept_relationship r on r.concept_id_1=an.concept_id -- connect to concept table
join VOCABULARY_20200910.dbo.concept c on c.concept_id=r.concept_id_2 and c.domain_id='Condition' -- Snomed Conditions
where an.concept_class_id in ('Ind / CI', 'Indication')
and de.vocabulary_id in ('RxNorm', 'RxNorm Extension')
and de.concept_class_id = 'Ingredient'
and c.concept_id in (381270, 378419) -- concept for the Parkinson's and Alzheimer's SNOMED codes


-- FINDING CONDITIONS THAT ARE INDICATIONS FOR SPECIFIC DRUGS
select distinct
  c.concept_id as c_id, c.concept_name as c_name, c.vocabulary_id as c_vocab, c.domain_id as c_domain, c.concept_class_id as c_class, -- Condition
  de.concept_id as de_id, de.concept_name as de_name, de.vocabulary_id as de_vocab, de.domain_id as de_domain, de.concept_class_id as de_class -- Drug
from VOCABULARY_20200910.dbo.concept an -- Indications
join VOCABULARY_20200910.dbo.concept_ancestor a on a.ancestor_concept_id=an.concept_id -- connect to
join VOCABULARY_20200910.dbo.concept de on de.concept_id=a.descendant_concept_id -- ...drug
join VOCABULARY_20200910.dbo.concept_relationship r on r.concept_id_1=an.concept_id -- connect to
join VOCABULARY_20200910.dbo.concept c on c.concept_id=r.concept_id_2 and c.domain_id='Condition' -- Snomed Conditions
where an.concept_class_id in ('Ind / CI', 'Indication')
and de.vocabulary_id in ('RxNorm', 'RxNorm Extension')
and de.concept_class_id = 'Ingredient'
and de.concept_id in (43526465) -- concept for canagliflozin (diabetes drug)
