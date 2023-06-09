# Start with reference tables imported
fullDbSetup()

test_that("Full data generation on CDM", {

  createCohorts(connection, cdmConfig)

  print("Checking target cohorts")
  qdf <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                    "SELECT count(*) as cohort_count FROM @schema.@cohort_table",
                                                    schema = cdmConfig$resultSchema,
                                                    cohort_table = cdmConfig$tables$cohort)
  initialCount <- qdf$COHORT_COUNT[[1]]
  # Target cohorts should be created
  expect_true(initialCount > 0)

  print("Checking regeneration")
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

  cohortDefinition <- RJSONIO::fromJSON(file.path("testCfg", "atlasExposureCohort1.json"))
  sqlDefinition <- readr::read_file(file.path("testCfg", "atlasCohort1.sql"))
  insertAtlasCohortRef(connection, config, 9999, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition, exposure = TRUE)
  exportReferenceTables(config, exportZipFile = zipFilePath)
  importReferenceTables(cdmConfig, zipFilePath)

  uncomputed <- getUncomputedAtlasCohorts(connection, cdmConfig, exposureCohorts = TRUE)

  expect_true(9999 %in% uncomputed$ATLAS_ID)
  expect_true(nrow(uncomputed) > 0)

  createCohorts(connection, cdmConfig)

  qdf <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                    "SELECT count(*) as cohort_count FROM @schema.@cohort_table",
                                                    schema = cdmConfig$resultSchema,
                                                    cohort_table = cdmConfig$tables$cohort)
  reCount2 <- qdf$COHORT_COUNT[[1]]
  # Should only run the cohorts once!
  expect_true(reCount2 > reCount)

  print("Checking outcome cohorts")
  createOutcomeCohorts(connection, cdmConfig)

  qdf <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                    "SELECT count(*) as cohort_count FROM @schema.@cohort_table",
                                                    schema = cdmConfig$resultSchema,
                                                    cohort_table = cdmConfig$tables$outcomeCohort)
  # Outcome cohorts should be created
  initialCount <- qdf$COHORT_COUNT[[1]]
  expect_true(initialCount > 0)

  print("Checking outcome cohorts regeneration")
  createOutcomeCohorts(connection, cdmConfig)

  qdf <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                    "SELECT count(*) as cohort_count FROM @schema.@cohort_table",
                                                    schema = cdmConfig$resultSchema,
                                                    cohort_table = cdmConfig$tables$outcomeCohort)
  reCount <- qdf$COHORT_COUNT[[1]]
  # Should only run the cohorts once!
  expect_true(initialCount == reCount)

  # Test adding a new atlas cohort - requires exporting references again
  cohortDefinition <- RJSONIO::fromJSON(file.path("testCfg", "atlasCohort1.json"))
  cohortDefinition$name <- "TEST 2" # requires changing so encoding isn't the same
  sqlDefinition <- readr::read_file(file.path("testCfg", "atlasCohort1.sql"))
  sqlDefinition <- paste(sqlDefinition, "SELECT 1;")
  insertAtlasCohortRef(connection, config, 10001, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

  exportReferenceTables(config)
  importReferenceTables(cdmConfig, zipFilePath)

  uncomputed <- getUncomputedAtlasCohorts(connection, cdmConfig, exposureCohorts = FALSE)

  expect_true(10001 %in% uncomputed$ATLAS_ID)
  expect_true(nrow(uncomputed) > 0)

  createOutcomeCohorts(connection, cdmConfig)

  qdf <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                    "SELECT count(*) as cohort_count FROM @schema.@cohort_table",
                                                    schema = cdmConfig$resultSchema,
                                                    cohort_table = cdmConfig$tables$outcomeCohort)
  reCount2 <- qdf$COHORT_COUNT[[1]]
  # Should only run the cohorts once!
  expect_true(initialCount < reCount2)

})