library(rewardb)
globalConfigPath <- "config/global-cfg.yml"
config <- loadGlobalConfiguration(globalConfigPath)
exportReferenceTables(config, exportPath = "rewardb-references.zip")

runDatabuildJob("config/cdm/mdcr.yml", globalConfigPath, rewardReferenceZipPath, name = "MDCR - reward full set", workingDir = ".")
runDatabuildJob("config/cdm/mdcd.yml", globalConfigPath, rewardReferenceZipPath, name = "MDCD - reward full set", workingDir = ".")
runDatabuildJob("config/cdm/ccae.yml", globalConfigPath, rewardReferenceZipPath, name = "CCAE - reward full set", workingDir = ".")
runDatabuildJob("config/cdm/jmdc.yml", globalConfigPath, rewardReferenceZipPath, name = "JMDC - reward full set", workingDir = ".")
runDatabuildJob("config/cdm/optum.yml", globalConfigPath, rewardReferenceZipPath, name = "optum - reward full set", workingDir = ".")
runDatabuildJob("config/cdm/pharmetrics.yml", globalConfigPath, rewardReferenceZipPath, name = "pharmetrics - reward full set", workingDir = ".")