if object_id('@cohort_database_schema.@outcome_summary_table', 'U') is not null
    drop table @cohort_database_schema.@outcome_summary_table;

CREATE TABLE @cohort_database_schema.@outcome_summary_table (
   target_cohort_definition_id   bigint,
   outcome_cohort_definition_id  bigint,
   at_risk_pp                    int,
   cases_pp                      int,
   pt_pp                         numeric(38,6),
   ip_pp                         numeric(24,12),
   ir_pp                         numeric(38,20),
   cases_itt                     int,
   pt_itt                        numeric(38,6),
   ip_itt                        numeric(24,12),
   ir_itt                        numeric(38,20)
);
