
#' Dashboard models
DashboardDbModel <- setRefClass("DashboardDbModel", contains = "DbModel")
DashboardDbModel$methods(
  initialize = function(...) {
    callSuper(...)
    setSchemaName(config$short_name)
  },

  getExposureControls = function(outcomeIds, minCohortSize = 10, sourceIds = NULL) {
    sql <- "
      SELECT r.*, o.type_id as outcome_type
      FROM @schema.result r
      INNER JOIN @schema.negative_control nc ON (
        r.target_cohort_id = nc.target_cohort_id AND nc.outcome_cohort_id = r.outcome_cohort_id
      )
      INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
      WHERE r.OUTCOME_COHORT_ID IN (@outcome_cohort_ids)
      AND r.calibrated = 0
      AND T_CASES >= @min_cohort_size
    "
    return(queryDb(sql, outcome_cohort_ids = outcomeIds, min_cohort_size = minCohortSize))
  },

  getOutcomeControls = function(targetIds, sourceIds = NULL, minCohortSize = 5, outcomeTypes = c(0, 1, 2), analysisId = 1) {
    sql <- "
      SELECT r.*, o.type_id as outcome_type
      FROM @schema.result r
      INNER JOIN @schema.negative_control nc ON (
        r.target_cohort_id = nc.target_cohort_id AND nc.outcome_cohort_id = r.outcome_cohort_id
      )
      INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
      AND r.calibrated = 0
      AND T_CASES >= @min_cohort_size
      AND o.type_id IN (@outcome_types)
      AND r.analysis_id IN (@analysis_id)
      {@source_ids != ''} ? {AND r.source_id IN (@source_ids)}
      {@exposure_ids != ''} ? {AND r.target_cohort_id IN (@exposure_ids)}
    "
    return(queryDb(sql,
                   exposure_ids = targetIds,
                   source_ids = sourceIds,
                   analysis_id = analysisId,
                   outcome_types = outcomeTypes,
                   min_cohort_size = minCohortSize))
  },

  getOutcomeCohortNames = function() {
    result <- queryDb("SELECT DISTINCT COHORT_NAME FROM @schema.outcome ORDER BY COHORT_NAME")
    return(result$COHORT_NAME)
  },

  getExposureCohortNames = function() {
    result <- queryDb("SELECT DISTINCT COHORT_NAME FROM @schema.target ORDER BY COHORT_NAME")
    return(result$COHORT_NAME)
  },

  getExposureClassNames = function() {
    result <- queryDb("SELECT DISTINCT EXPOSURE_CLASS_NAME FROM @schema.exposure_class ORDER BY EXPOSURE_CLASS_NAME")
    return(result$EXPOSURE_CLASS_NAME)
  },

  getFilteredTableResultsQuery = function(benefitThreshold = 0.5,
                                          riskThreshold = 2.0,
                                          pValueCut = 0.05,
                                          requiredBenefitSources = NULL,
                                          filterByMeta = FALSE,
                                          outcomeCohortTypes = c(0, 1, 2),
                                          calibrated = TRUE,
                                          benefitCount = 1,
                                          riskCount = 0,
                                          targetCohortNames = NULL,
                                          outcomeCohortNames = NULL,
                                          exposureClasses = NULL,
                                          orderByCol = NULL,
                                          ascending = NULL,
                                          limit = NULL,
                                          offset = NULL) {
    calibrated <- ifelse(calibrated, 1, 0)
    filterOutcomes <- length(outcomeCohortTypes) > 0

    sql <- readr::read_file(system.file("sql/queries/", "mainTable.sql", package = "rewardb"))
    query <- SqlRender::render(
      sql,
      risk = riskThreshold,
      benefit = benefitThreshold,
      p_cut_value = pValueCut,
      filter_outcome_types = filterOutcomes,
      outcome_types = outcomeCohortTypes,
      risk_count = riskCount,
      benefit_count = benefitCount,
      calibrated = calibrated,
      show_exposure_classes = config$useExposureControls,
      filter_by_meta_analysis = filterByMeta,
      outcome_cohort_name_length = length(outcomeCohortNames) > 0,
      outcome_cohort_names = outcomeCohortNames,
      target_cohort_name_length = length(targetCohortNames) > 0,
      target_cohort_names = targetCohortNames,
      exposure_classes = exposureClasses,
      required_benefit_sources = requiredBenefitSources,
      required_benefit_count = length(requiredBenefitSources),
      order_by = orderByCol,
      ascending = ascending,
      limit = limit,
      offset = offset
    )
    return(query)
  },

  getFilteredTableResults = function(...) {
    sql <- getFilteredTableResultsQuery(...)
    queryDb(sql)
  },

  getFilteredTableResultsCount = function(...) {
    sql <- getFilteredTableResultsQuery(...)
    countQuery(sql, render = FALSE)
  },

  getNegativeControls = function() {
    sql <- readr::read_file(system.file("sql/export/", "negativeControls.sql", package = "rewardb"))
    df <- queryDb(sql)
    return(df)
  },

  getMappedAssociations = function() {
    sql <- readr::read_file(system.file("sql/export/", "mappedIndications.sql", package = "rewardb"))
    df <- queryDb(sql)
    return(df)
  },

  getMetaAnalysisTable = function(exposureId, outcomeId, calibrationType = NULL, sourceIds = NULL) {
    sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRowsGrouped.sql", package = "rewardb"))
    return(queryDb(sql, treatment = exposureId, outcome = outcomeId))
  },

  getForestPlotTable = function(exposureId, outcomeId, calibrated, calibrationType = NULL, sourceIds = NULL) {
    sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRows.sql", package = "rewardb"))
    table <- queryDb(sql, treatment = exposureId, outcome = outcomeId, calibrated = calibrated)
    calibratedTable <- table[table$CALIBRATED == 1,]
    uncalibratedTable <- table[table$CALIBRATED == 0,]

    if (nrow(calibratedTable) & nrow(uncalibratedTable)) {
      calibratedTable$calibrated <- "Calibrated"
      uncalibratedTable$calibrated <- "Uncalibrated"
      uncalibratedTable$SOURCE_NAME <- paste0(uncalibratedTable$SOURCE_NAME, "\n uncalibrated")
      calibratedTable$SOURCE_NAME <- paste0(calibratedTable$SOURCE_NAME, "\n Calibrated")
    }

    table <- rbind(uncalibratedTable[order(uncalibratedTable$SOURCE_ID, decreasing = TRUE),],
                   calibratedTable[order(calibratedTable$SOURCE_ID, decreasing = TRUE),])
    return(table)
  },

  getTimeOnTreatmentStats = function(...) {
    dt <- callSuper(..., tableName = "result")
    return(dt)
  },

  getTimeToOutcomeStats = function(...) {
    dt <- callSuper(..., tableName = "result")
    return(dt)
  },

  getFullDataSet = function(calibrated = c(0, 1)) {
    sql <- readr::read_file(system.file("sql/export/", "fullDashboardData.sql", package = "rewardb"))
    df <- queryDb(sql, calibrated = calibrated, show_exposure_classes = config$useExposureControls)
    return(df)
  },

  getOutcomeType = function(outcomeId) {
    res <- queryDb("SELECT type_id FROM @schema.outcome where outcome_cohort_id = @outcome", outcome = outcomeId)
    return(res$TYPE_ID[[1]])
  },

  getExposureOutcomeRows = function(exposure, outcome, calibrated) {
    sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRows.sql", package = "rewardb"))
    queryDb(sql, treatment = exposure, outcome = outcome, calibrated = calibrated)
  },

  getOutcomeConceptSet = function(outcomeId) {
    queryDb("SELECT c.concept_name,
             oc.condition_concept_id as concept_id,
             include_descendants,
             is_excluded
             from @schema.outcome_concept oc
             inner join @vocabulary_schema.concept c on c.concept_id = oc.condition_concept_id
             WHERE outcome_cohort_id = @outcome",
            outcome = outcomeId,
            vocabulary_schema = config$globalConfig$vocabularySchema,
            snakeCaseToCamelCase = TRUE)
  },

  getExposureConceptSet = function(exposureId) {
    queryDb("SELECT c.concept_name,
             tc.concept_id,
             include_descendants,
             is_excluded
             from @schema.target_concept tc
             inner join @vocabulary_schema.concept c on c.concept_id = tc.concept_id
             WHERE target_cohort_id = @exposure",
            exposure = exposureId,
            vocabulary_schema = config$globalConfig$vocabularySchema,
            snakeCaseToCamelCase = TRUE)
  }
)
