devtools::load_all()
library(MethodEvaluation)
# Ad cohorts to results

config <- loadGlobalConfig("config/global-cfg.yml")
cdmConfigPaths <-c(
  #"config/cdm/pharmetrics.yml",
  "config/cdm/mcdc.yml",
  "config/cdm/mcdr.yml",
  "config/cdm/ccae.yml",
  "config/cdm/optum.yml"
)

connection <- DatabaseConnector::connect(config$connectionDetails)
configId <- "exposure-control-evaluation"

outcomes <- list()
adHocCohortIds <- c()


benchmarkOutcomeCohorts <- data.frame(sqlName = c("acute_pancreatitis", "gi_bleed", "stroke", "ibd"),
                                      name = c("[Benchmark] Acute Pancreatitis", "[Benchmark] GI Bleed", "[Benchmark] stroke", "[Benchmark] ibd"),
                                      cohortId = c(1, 2, 3, 4))

sourceUrl <- "OHDSI/MethodsLibrary"

for (i in 1:nrow(benchmarkOutcomeCohorts)) {
  ParallelLogger::logInfo(paste("Creating outcome:", benchmarkOutcomeCohorts$sqlName[i]))

  sqlFilename <- paste0(benchmarkOutcomeCohorts$sqlName[i], ".sql")
  
  sqlFile <- system.file(file.path("sql", "sql_server", sqlFilename), package = "MethodEvaluation")
  sqlDefinition <- readr::read_file(sqlFile)
  
  jsonFilename <- paste0(benchmarkOutcomeCohorts$sqlName[i], ".json")
  jsonFile <- system.file(file.path("cohorts", jsonFilename), package = "MethodEvaluation")
  cohortDefinition <- list()
  jsonDef <- jsonlite::read_json(jsonFile)
  
  cohortDefinition$name <- benchmarkOutcomeCohorts$name[i]
  cohortDefinition$description <- "OHDSI exposure negative control benchmark outcome - OHDSI/MethodEvaluation"
  cohortDefinition$id <- i
  cohortDefinition$expression <- jsonDef
  
  removeAtlasCohort(connection, config, i, webApiUrl = sourceUrl)
  
  insertAtlasCohortRef(connection,
                       config,
                       i,
                       webApiUrl= sourceUrl,
                       cohortDefinition = cohortDefinition,
                       sqlDefinition = sqlDefinition)
}


zipFilePath <- "rewardb-references.zip"
exportReferenceTables(config, exportZipFile = zipFilePath)

for (path in cdmConfigPaths) {
  cdmConfig <- loadCdmConfig(path)
  importReferenceTables(cdmConfig, zipFilePath = zipFilePath)
  resultsFiles <- sccAdHocCohorts(path, configId, c(1, 2, 3, 4), sourceUrl = sourceUrl)
  for (table in names(resultsFiles)) {
    for (sqlFile in resultsFiles[[table]]) {
      pgCopy(config$connectionDetails, sqlFile, config$rewardbResultsSchema, table)
    }
  }
}

# Get exposure controls for 4 outcomes
data("ohdsiNegativeControls", package = 'MethodEvaluation')
ohdsiNegativeControls <- ohdsiNegativeControls[ohdsiNegativeControls$type == "Exposure control",]

ohdsiControlsMapping <- data.frame(
  exposureConceptId = c(ohdsiNegativeControls$targetId, ohdsiNegativeControls$nestingId),
  atlasId = c(ohdsiNegativeControls$outcomeId, ohdsiNegativeControls$outcomeId),
  atlasUrl = sourceUrl
)


manualNegativeData <- getConceptCohortDataFromAtlasOutcomes(connection, config, ohdsiControlsMapping)
automatedExposureControls <- pass() # Map negative controls from ato the atlas outcomes
automatedExposureControlData <- getConcetptCohortData(connection, config, automatedOutcomeControls)
