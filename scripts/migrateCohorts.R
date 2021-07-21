devtools::load_all()

oldAtlas <- "https://awsagunva1011.jnj.com:8443/WebAPI"
newAtlas <- "https://epi.jnj.com:8443/WebAPI"

ROhdsiWebApi::authorizeWebApi(newAtlas, "windows", "jgilber2", keyring::key_get("jnj", "jgilber2"))

oldConfig <- loadGlobalConfiguration("config/global-cfg-feb2021.yml")
# Get current cohorts from old atlas version

run <- function() {
  connection <- DatabaseConnector::connect(connectionDetails = oldConfig$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  cohorts <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                        "SELECT atlas_id FROM @schema.atlas_outcome_reference WHERE atlas_url = 'https://awsagunva1011.jnj.com:8443/WebAPI'",
                                                        schema = oldConfig$rewardbResultsSchema,
                                                        snakeCaseToCamelCase = TRUE)


  pattern <- "(\\[All-by-all\\])|(\\[Phenotype Library #\\d+\\])|(\\[PL \\d+\\])|(\\[REWARD\\])"
  
  cohortsNewIds <- tibble::tibble()
  for (rid in 1:nrow(cohorts)) {
    cohortId <- cohorts$atlasId[rid]
    cohortDefinition <- ROhdsiWebApi::getCohortDefinition(cohortId, oldAtlas)

    if (any(grep(pattern, cohortDefinition$name) > 0)) {
      name <- gsub(pattern, "[REWARD]", cohortDefinition$name)
    } else {
      name <- paste("[REWARD]", cohortDefinition$name)
    }
    print(name)

    tryCatch({
      addCohort <- ROhdsiWebApi::postCohortDefinition(name = name, cohortDefinition = cohortDefinition, baseUrl = newAtlas)
      cohortsNewIds <- rbind(cohortsNewIds, addCohort)
    }, error = function(err) {
      print(paste("https://awsagunva1011.jnj.com/atlas/#/cohortdefinition/", cohortId))
    })
  }
  cohortsNewIds
}

addIds <- run()