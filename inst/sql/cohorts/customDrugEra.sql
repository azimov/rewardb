-- Create custom drug eras
IF object_id('@drug_era_schema.drug_era', 'U') is not null
    drop table @drug_era_schema.drug_era;

IF OBJECT_ID('@drug_era_schema.drug_era_info', 'U') IS NOT NULL
    drop table @drug_era_schema.drug_era_info;

create table @drug_era_schema.drug_era_info (
  ingredient_concept_id int NOT NULL,
	ingredient_name varchar(50) NOT NULL,
	maintenance_days int NOT NULL)
;

/*
ingredient_concept_id	ingredient_name	maintenance_days
937368								infliximab			56
19041065							golimumab				30
1151789								etanercept			7
912263								certolizumab pegol	28
1119119								adalimumab			14
1593700								guselkumab 			56
40161532							ustekinumab 		84
45892883							secukinumab 		28
35603563							ixekizumab			28
1592513								brodalumab 			14
35200139							tildrakizumab 	84
1511348								risankizmab 		84
*/

insert into @drug_era_schema.drug_era_info(ingredient_concept_id, ingredient_name, maintenance_days) VALUES (937368, 'infliximab', 56); -- infliximab
insert into @drug_era_schema.drug_era_info(ingredient_concept_id, ingredient_name, maintenance_days) VALUES (19041065, 'golimumab', 30); -- golimumab
insert into @drug_era_schema.drug_era_info(ingredient_concept_id, ingredient_name, maintenance_days) VALUES (1151789, 'etanercept', 7); -- brodalumab
insert into @drug_era_schema.drug_era_info(ingredient_concept_id, ingredient_name, maintenance_days) VALUES (912263, 'certolizumab pegol', 28); -- brodalumab
insert into @drug_era_schema.drug_era_info(ingredient_concept_id, ingredient_name, maintenance_days) VALUES (1119119, 'adalimumab', 14); -- brodalumab
insert into @drug_era_schema.drug_era_info(ingredient_concept_id, ingredient_name, maintenance_days) VALUES (1593700, 'guselkumab', 56); -- brodalumab
insert into @drug_era_schema.drug_era_info(ingredient_concept_id, ingredient_name, maintenance_days) VALUES (40161532, 'ustekinumab', 84); -- brodalumab
insert into @drug_era_schema.drug_era_info(ingredient_concept_id, ingredient_name, maintenance_days) VALUES (45892883, 'secukinumab', 28); -- brodalumab
insert into @drug_era_schema.drug_era_info(ingredient_concept_id, ingredient_name, maintenance_days) VALUES (35603563, 'ixekizumab', 28); -- brodalumab
insert into @drug_era_schema.drug_era_info(ingredient_concept_id, ingredient_name, maintenance_days) VALUES (1592513, 'brodalumab', 14); -- brodalumab
insert into @drug_era_schema.drug_era_info(ingredient_concept_id, ingredient_name, maintenance_days) VALUES (35200139, 'tildrakizumab', 84); -- tildrakizumab
insert into @drug_era_schema.drug_era_info(ingredient_concept_id, ingredient_name, maintenance_days) VALUES (1511348, 'risankizumab', 84); -- risankizumab

-- select * from @drug_era_schema.drug_era_info

--- Notes: we use the maintenance_days to define the drug exposure duration, but we allow a 30d gap window between exposures to considder them together.
--- if a 0 day grace period is required, change the DATEADD(day,-30,EVENT_DATE) and DATEADD(day,30,EVENT_DATE) to add/subtract 0 days.

--HINT DISTRIBUTE_ON_KEY(person_id)
create table #reward_drug_era
as
with cteDrugTarget (DRUG_EXPOSURE_ID, PERSON_ID, DRUG_CONCEPT_ID, DRUG_TYPE_CONCEPT_ID, DRUG_EXPOSURE_START_DATE, DRUG_EXPOSURE_END_DATE, INGREDIENT_CONCEPT_ID) as
(
-- Define drug exposure end dates by using the DRUGS_OF_INTEREST.maintenance_days
	select d.DRUG_EXPOSURE_ID, d. PERSON_ID, c.CONCEPT_ID, d.DRUG_TYPE_CONCEPT_ID, DRUG_EXPOSURE_START_DATE,
		DATEADD(day,DOI.maintenance_days,DRUG_EXPOSURE_START_DATE) as DRUG_EXPOSURE_END_DATE,
		c.CONCEPT_ID as INGREDIENT_CONCEPT_ID
	from @cdm_database.DRUG_EXPOSURE d
		join @cdm_database.CONCEPT_ANCESTOR ca on ca.DESCENDANT_CONCEPT_ID = d.DRUG_CONCEPT_ID
		join @cdm_database.CONCEPT c on ca.ANCESTOR_CONCEPT_ID = c.CONCEPT_ID
		join @drug_era_schema.drug_era_info doi on doi.INGREDIENT_CONCEPT_ID = c.concept_id
		where c.CONCEPT_CLASS_ID = 'Ingredient'
),
cteEndDates (PERSON_ID, INGREDIENT_CONCEPT_ID, END_DATE) as -- the magic
(
	select PERSON_ID, INGREDIENT_CONCEPT_ID, DATEADD(day,-30,EVENT_DATE) as END_DATE -- unpad the end date
	from
	(
		select PERSON_ID, INGREDIENT_CONCEPT_ID, EVENT_DATE, EVENT_TYPE,
		max(START_ORDINAL) over (partition by PERSON_ID, INGREDIENT_CONCEPT_ID order by EVENT_DATE, EVENT_TYPE rows unbounded preceding) as START_ORDINAL, -- this pulls the current START down from the prior rows so that the NULLs from the END DATES will contain a value we can compare with
		row_number() over (partition by PERSON_ID, INGREDIENT_CONCEPT_ID order by EVENT_DATE, EVENT_TYPE) as OVERALL_ORD -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
		from
		(
			-- select the start dates, assigning a row number to each
			select PERSON_ID, INGREDIENT_CONCEPT_ID, DRUG_EXPOSURE_START_DATE as EVENT_DATE, -1 as EVENT_TYPE, row_number() over (partition by PERSON_ID, DRUG_CONCEPT_ID order by DRUG_EXPOSURE_START_DATE) as START_ORDINAL
			from cteDrugTarget

			union all

			-- pad the end dates by 30 to allow a grace period for overlapping ranges.
			select PERSON_ID, INGREDIENT_CONCEPT_ID, DATEADD(day,30,DRUG_EXPOSURE_END_DATE), 1 as EVENT_TYPE, null
			from cteDrugTarget
		) RAWDATA
	) E
	where (2 * E.START_ORDINAL) - E.OVERALL_ORD = 0
),
cteDrugExposureEnds (PERSON_ID, DRUG_CONCEPT_ID, DRUG_TYPE_CONCEPT_ID, DRUG_EXPOSURE_START_DATE, DRUG_ERA_END_DATE) as
(
select
	d.PERSON_ID,
	d.INGREDIENT_CONCEPT_ID,
	d.DRUG_TYPE_CONCEPT_ID,
	d.DRUG_EXPOSURE_START_DATE,
	min(e.END_DATE) as ERA_END_DATE
from cteDrugTarget d
join cteEndDates e on d.PERSON_ID = e.PERSON_ID and d.INGREDIENT_CONCEPT_ID = e.INGREDIENT_CONCEPT_ID and e.END_DATE >= d.DRUG_EXPOSURE_START_DATE
group by d.DRUG_EXPOSURE_ID,
	d.PERSON_ID,
	d.INGREDIENT_CONCEPT_ID,
	d.DRUG_TYPE_CONCEPT_ID,
	d.DRUG_EXPOSURE_START_DATE
)
select person_id, drug_concept_id, min(DRUG_EXPOSURE_START_DATE) as DRUG_ERA_START_DATE, DRUG_ERA_END_DATE, count(*) as DRUG_EXPOSURE_COUNT
from cteDrugExposureEnds
group by person_id, drug_concept_id, drug_type_concept_id, DRUG_ERA_END_DATE
;

--select distinct drug_concept_id from #reward_drug_era;
--HINT DISTRIBUTE_ON_KEY(person_id)
create table @drug_era_schema.drug_era
as
select (select max(drug_era_id) from @cdm_database.drug_era) + row_number() over (order by (select 1)) as drug_era_id, person_id, drug_concept_id, drug_era_start_date, drug_era_end_date, drug_exposure_count
from #reward_drug_era
union all
select drug_era_id, person_id, drug_concept_id, drug_era_start_date, drug_era_end_date, drug_exposure_count
from @cdm_database.drug_era
where drug_concept_id not in (select ingredient_concept_id from @drug_era_schema.drug_era_info);

-- select count(distinct drug_concept_id) from @drug_era_schema.drug_era

drop table #reward_drug_era;


