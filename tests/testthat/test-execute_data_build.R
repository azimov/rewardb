test_that("create reference tables", {
  config <- yaml::read_yaml(system.file("tests", "test.cfg.yml", package = "rewardb"))
  connection <- DatabaseConnector::connect(config$cdmDataSource)

  dataSources <- rewardb::checkDataSources(NULL, config)
  ParallelLogger::logInfo("Creating and populating reference tables...")
  rewardb::createReferenceTables(connection, config, names(dataSources))

})
