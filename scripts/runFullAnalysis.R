devtools::load_all()

config <- loadGlobalConfig("config/global-cfg.yml")

exportReferenceTables(config)

cdmConfigPaths <- c(
  "config/cdm/pharmetrics.yml",
  "config/cdm/mcdc.yml",
  "config/cdm/mcdr.yml",
  "config/cdm/ccae.yml",
  "config/cdm/optum.yml"
)

for (cdmConfigPath in cdmConfigPaths) {
  cdmConfig <- loadCdmConfig(cdmConfigPath)
  importReferenceTables(cdmConfig, "rewardb-references.zip")
  resultsFiles <- generateSccResults(cdmConfigPath)
  # Copy files
  for (table in names(resultsFiles)) {
    for (file in resultsFiles[[table]]) {
      pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, table, fileEncoding = "UTF-8-BOM")
    }
  }
}