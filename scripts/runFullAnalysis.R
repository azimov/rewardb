devtools::load_all()

config <- loadGlobalConfig("config/global-cfg.yml")

exportReferenceTables(config)

cdmConfigPaths <- c(

  "config/cdm/mdcd.yml",
  "config/cdm/mdcr.yml",
  "config/cdm/ccae.yml",
  "config/cdm/optum.yml",
  "config/cdm/pharmetrics.yml"
)

for (cdmConfigPath in cdmConfigPaths) {
  cdmConfig <- loadCdmConfig(cdmConfigPath)
  importReferenceTables(cdmConfig, "rewardb-references.zip")
  createCustomDrugEras(cdmConfigPath)
  resultsFiles <- generateSccResults(cdmConfigPath)
  # Copy files
  for (table in names(resultsFiles)) {
    for (file in resultsFiles[[table]]) {
      pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, table, fileEncoding = "UTF-8-BOM")
    }
  }
}