#' Insert simulated drug eras Helper
#' This can be used to ensure that the exposures required are captured by:
#'  a) Cohort building process
#'  b) Cohort
insertSimulatedExposure <- function(connection,
                                    outcomeConceptId,
                                    exposureConceptId,
                                    nExposedTime,
                                    nUnexposedTime,
                                    minExposureLength = 30,
                                    maxExposureLength = 180,
                                    cdmSchema = "eunomia") {


  sql <- "
  SELECT * FROM (
    SELECT person_id,
           condition_era_start_date,
           condition_era_end_date,
           row_number() over (partition by person_id order by condition_era_start_date asc) row_num
           FROM @cdm_schema.condition_era
           WHERE condition_concept_id = @outcome_concept_id
           ) sq
    WHERE sq.row_num = 1
          "
  patientsWithOutcome <- renderTranslateQuerySql(connection,
                                                 sql,
                                                 cdm_schema = cdmSchema,
                                                 outcome_concept_id = outcomeConceptId,
                                                 snakeCaseToCamelCase = TRUE)

  patientIds <- unique(patientsWithOutcome$personId)
  if (length(patientIds) < nExposedTime + nUnexposedTime) {
    stop("Insufficient patients to insert data")
  }

  # Sample selection of unique patients
  selectedPatients <- sample(patientIds, nExposedTime + nUnexposedTime, replace = FALSE)
  patientsSample <- patientsWithOutcome %>%
    filter(personId %in% selectedPatients) %>%
    group_by(personId) %>%
    mutate(conditionEraStartDate)

  patientsWithOutcomeExposed <- patientsSample[1:nExposedTime, ]
  patientsWithOutcomeUnexposed <- patientsSample[(nExposedTime + 1):nrow(patientsSample), ]

  # outcomes in exposed time
  exposureLengths <- sample(minExposureLength:maxExposureLength, nExposedTime, replace = TRUE)
  # gap between exposure and outcome
  exposureOutcomeGapTime <- sapply(exposureLengths, function(x) { sample(0:x, 1)})

  # Start date is random date less than exposureLength before condition start
  patientsWithOutcomeExposed$drugEraStartDate <- patientsWithOutcomeExposed$conditionEraStartDate + exposureOutcomeGapTime
  patientsWithOutcomeExposed$drugEraEndDate <- patientsWithOutcomeExposed$drugEraStartDate + exposureLengths

  unexposureLengths <- sample(minExposureLength:maxExposureLength, nUnexposedTime, replace = TRUE)
  # gap between exposure and outcome
  unexposureOutcomeGapTime <- sapply(unexposureLengths, function(x) { sample(1:x, 1)})

  patientsWithOutcomeUnexposed$drugEraStartDate <- patientsWithOutcomeUnexposed$conditionEraEndDate - unexposureOutcomeGapTime
  patientsWithOutcomeUnexposed$drugEraEndDate <- patientsWithOutcomeUnexposed$drugEraStartDate + unexposureLengths

  insertData <- rbind(patientsWithOutcomeExposed, patientsWithOutcomeUnexposed) %>%
    mutate(drugConceptId = exposureConceptId, drugExposureCount = 1) %>%
    select(personId, drugEraStartDate, drugEraEndDate, drugConceptId)

  DatabaseConnector::insertTable(connection,
                                 data = insertData,
                                 databaseSchema = cdmSchema,
                                 tableName = "drug_era",
                                 camelCaseToSnakeCase = TRUE,
                                 dropTableIfExists = FALSE,
                                 createTable = FALSE)
}