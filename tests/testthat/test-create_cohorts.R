configFilePath <- system.file("tests", "test.cfg.yml", package = "rewardb")
config <- yaml::read_yaml(configFilePath)
connection <- DatabaseConnector::connect(config$rewardbDatabase)

cdmConfigPath <- system.file("tests", "eunomia.cdm.cfg.yml", package = "rewardb")
cdmConfig <- yaml::read_yaml(cdmConfigPath)

# Set up a database with constructed cohorts etc
buildPgDatabase(configFilePath = configFilePath)
cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasCohort12047.json", package = "rewardb"))
sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort12047.sql", package = "rewardb"))
insertAtlasCohortRef(connection, config, 12047, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

conceptSetId <- 11933
conceptSetDefinition <- RJSONIO::fromJSON(system.file("tests", "conceptSet1.json", package = "rewardb"))
insertCustomExposureRef(connection, config, conceptSetId, "Test Exposure Cohort", conceptSetDefinition = conceptSetDefinition)


zipFilePath <- "rewardb-references.zip"
refFolder <- "reference_test_folder"
unlink(zipFilePath)
unlink(refFolder)
exportReferenceTables(config)
importReferenceTables(cdmConfig, zipFilePath, refFolder)

test_that("Full data generation on CDM", {

  createCohorts(connection, cdmConfig)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as cohort_count FROM @schema.@cohort_table",
    schema = cdmConfig$resultSchema,
    cohort_table = cdmConfig$tables$cohort
  )
  initialCount <- qdf$COHORT_COUNT[[1]]
  # Target cohorts should be created
  expect_true(initialCount > 0)

  createCohorts(connection, cdmConfig)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as cohort_count FROM @schema.@cohort_table",
    schema = cdmConfig$resultSchema,
    cohort_table = cdmConfig$tables$cohort
  )
  reCount <- qdf$COHORT_COUNT[[1]]
  # Should only run the cohorts once!
  expect_true(initialCount == reCount)

  createOutcomeCohorts(connection, cdmConfig)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as cohort_count FROM @schema.@cohort_table",
    schema = cdmConfig$resultSchema,
    cohort_table = cdmConfig$tables$outcomeCohort
  )
  # Outcome cohorts should be created
  initialCount <- qdf$COHORT_COUNT[[1]]
  expect_true(initialCount > 0)
  createOutcomeCohorts(connection, cdmConfig)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as cohort_count FROM @schema.@cohort_table",
    schema = cdmConfig$resultSchema,
    cohort_table = cdmConfig$tables$outcomeCohort
  )
  reCount <- qdf$COHORT_COUNT[[1]]
  # Should only run the cohorts once!
  expect_true(initialCount == reCount)

  # Test adding a new atlas cohort - requires exporting references again
  cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasCohort1.json", package = "rewardb"))
  sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort1.sql", package = "rewardb"))
  insertAtlasCohortRef(connection, config, 1, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

  exportReferenceTables(config)
  importReferenceTables(cdmConfig, zipFilePath, refFolder)

  uncomputed <- getUncomputedAtlasCohorts(connection, cdmConfig)
  expect_true(1 %in% uncomputed$ATLAS_ID)
  expect_true(nrow(uncomputed) > 0)

  createOutcomeCohorts(connection, cdmConfig)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as cohort_count FROM @schema.@cohort_table",
    schema = cdmConfig$resultSchema,
    cohort_table = cdmConfig$tables$outcomeCohort
  )
  reCount2 <- qdf$COHORT_COUNT[[1]]
  # Should only run the cohorts once!
  expect_true(initialCount < reCount2)
})

DatabaseConnector::disconnect(connection)
unlink(zipFilePath)
unlink(refFolder)