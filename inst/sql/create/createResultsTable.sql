if object_id('@results_database_schema.@results_table', 'U') is not null
	drop table @results_database_schema.@results_table
;

create table @results_database_schema.@results_table
(
  exposureId bigint
  , outcomeId bigint
  , numPersons int
  , numExposures int
  , numOutcomesExposed int
  , numOutcomesUnexposed int
  , timeAtRiskExposed float
  , timeAtRiskUnexposed float
  , irr float
  , irrLb95 float
  , irrUb95 float
  , logRr float
  , seLogRr float
  , p float
)
with (distribution = replicate)
;
