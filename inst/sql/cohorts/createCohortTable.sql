if object_id('@cohort_database_schema.@cohort_table', 'U') is not null
	drop table @cohort_database_schema.@cohort_table;

create table @cohort_database_schema.@cohort_table
(
	cohort_definition_id bigint
	,	subject_id bigint
	,	cohort_start_date date
	,	cohort_end_date date
) with (distribution = hash(subject_id))
;
