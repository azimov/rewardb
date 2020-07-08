createTargetDefinitions <- function (connection, config) {

    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "sql/cohorts/getIngredientsAndATC.sql",
      packageName = "rewardb",
      vocabulary_database_schema = config$vocabularySchema
    )
    ingredients <- DatabaseConnector::querySql(connection, sql)

    conceptDefinitions <- ingredients[c("CONCEPT_ID", "CONCEPT_NAME", "CONCEPT_ID")]
    conceptDefinitions$ISEXCLUDED <- 0
    conceptDefinitions$INCLUDEDESCENDANTS <- 1
    conceptDefinitions$INCLUDEMAPPED <- 0
    conceptDefinitions$CONCEPTSET_ID <- ingredients$CONCEPT_ID

    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = "DELETE FROM @schema.@concept_set_definition_table",
      schema = config$schema,
      concept_set_definition_table = config$conceptSetDefinitionTable
    )
    DatabaseConnector::insertTable(connection, paste(config$schema, ".", config$conceptSetDefinitionTable), conceptDefinitions)

    #### cohort definitions ############################
    # COHORT_DEFINITION_ID, COHORT_DEFINITION_NAME, SHORT_NAME, DRUG_CONCEPTSET_ID, INDICATION_CONCEPTSET_ID, TARGET_COHORT,SUBGROUP_COHORT, ATC_FLG)
    # VALUES (1353766000,'New users of propranolol','New users of propranolol',1353766,0,0,0, 0)
    ingredients$COHORT_DEFINITION_ID <- ingredients$CONCEPT_ID * 1000 #COHORT_DEFINITION_ID
    colnames(ingredients)[colnames(ingredients) == "CONCEPT_NAME"] <- "COHORT_DEFINITION_NAME"
    colnames(ingredients)[colnames(ingredients) == "VOCABULARY_ID"] <- "SHORT_NAME"
    colnames(ingredients)[colnames(ingredients) == "CONCEPT_ID"] <- "DRUG_CONCEPTSET_ID"
    ingredients$INDICATION_CONCEPTSET_ID <- 0 # Not sure what this is used for but appears to always be 0
    ingredients$TARGET_COHORT <- 0
    ingredients$SUBGROUP_COHORT <- 0

    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = "DELETE FROM @schema.@cohort_definition_table",
      schema = config$schema,
      cohort_definition_table = config$cohortDefinitionTable
    )
    DatabaseConnector::insertTable(connection, paste(config$schema, ".", config$cohortDefinitionTable), ingredients)
}

createAtlasReference <- function (connection, config, dataSource, customOutcomeCohortList) {
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "sql/create/customAtalsCohorts.sql",
      packageName = "rewardb",
      dbms = attr(connection, "dbms"),
      cdm_database_schema = dataSource$cdmDatabaseSchema,
      cohort_database_schema = config$schema,
      outcome_cohort_definition_table = config$outcomeCohortDefinitionTable,
      cdm_outcome_cohort_schema = dataSource$cdmOutcomeCohortSchema,
      cdm_outcome_cohort_table = dataSource$cdmOutcomeCohortTable,
      custom_outcome_cohort_list = customOutcomeCohortList
    )
    DatabaseConnector::executeSql(connection, sql)
}

createReferenceTables <- function(connection, config)
{
    connection <- DatabaseConnector::connect(config$cdmDataSource)
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "sql/create/createReferenceTables.sql",
      packageName = "rewardb",
      dbms = attr(connection, "dbms"),
      cohort_database_schema = config$schema,
      conceptset_definition_table = config$conceptSetDefinitionTable,
      cohort_definition_table = config$cohortDefinitionTable,
      outcome_cohort_definition_table = config$outcomeCohortDefinitionTable
    )
    DatabaseConnector::executeSql(connection, sql)

    base::writeLines("Inserting ingredient/ATC cohorts")
    createTargetDefinitions(connection, config)

    for (dataSource in config$dataSources) {
        sql <- SqlRender::loadRenderTranslateSql(
          sqlFilename = "sql/create/outcomeCohortDefinitions.sql",
          packageName = "rewardb",
          dbms = attr(connection, "dbms"),
          cdm_database_schema = dataSource$cdmDatabaseSchema,
          cohort_database_schema = config$schema,
          outcome_cohort_definition_table = config$outcomeCohortDefinitionTable,
          cdm_outcome_cohort_schema = dataSource$cdmOutcomeCohortSchema,
          cdm_outcome_cohort_table = dataSource$cdmOutcomeCohortTable
        )
        DatabaseConnector::executeSql(connection, sql)

        # TODO - move these to a reference table to allow adding them one by one
        customOutcomeCohortList      <- c(7542, 7551, 7552, 7553, 7576, 7543, 7545, 7546, 7507, 7547, 7548, 7549, 7550, 7822, 7823, 10357,11073,
                                  2538, 10593, 10605, 15078, 10607, 11643, 12047)

        createAtlasReference(connection, config, dataSource, customOutcomeCohortList)
    }
}


execute <- function (configFilePath) {
    # load config
    base::writeLines("Creating and populating reference tables...")
    config <- yaml::read_yaml(configFilePath)
    # createReferenceTables
    createReferenceTables(connection, config)

    # createCohorts

    #for (dataSource in config$dataSources) {

      # createCohorts

      # createOutcomeCohorts

      # createOutcomeSummary

      # run SCC

      # addCustomSccOutcome

    #}
}

# Combine results

# compile results