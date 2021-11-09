
ReportDbModel <- setRefClass("ReportDbModel", contains = "DbModel")
ReportDbModel$methods(
  initialize = function(...) {
    callSuper(...)
    setSchemaName(config$rewardbResultsSchema)
  },

  getExposureCohort = function(cohortId) {
    getFirst("SELECT * FROM @schema.cohort_definition WHERE cohort_definition_id = @cohort_id", cohort_id = cohortId)
  },

  getOutcomeCohort = function(cohortId) {
    getFirst("SELECT * FROM @schema.outcome_cohort_definition WHERE cohort_definition_id = @cohort_id", cohort_id = cohortId)
  },

  getOutcomeCohorts = function() {
    sql <- "
    SELECT cd.* FROM @schema.outcome_cohort_definition cd
    INNER JOIN (
      SELECT outcome_cohort_id, count(*) FROM @schema.scc_result sr
      WHERE t_cases + c_cases > 10
      GROUP BY outcome_cohort_id
    ) sq ON sq.outcome_cohort_id = cd.cohort_definition_id
    ORDER BY cohort_definition_name"

    result <- cacheQuery("outcomeCohortsCounted", sql, snakeCaseToCamelCase = TRUE)
  },

  getExposureCohorts = function() {
    sql <- "
    SELECT cd.* FROM @schema.cohort_definition cd
    INNER JOIN (
      SELECT target_cohort_id, count(*) FROM @schema.scc_result sr
      WHERE t_cases + c_cases > 10
      GROUP BY target_cohort_id
    ) sq ON sq.target_cohort_id = cd.cohort_definition_id
    ORDER BY cohort_definition_name"

    result <- cacheQuery("exposureCohortsCounted", sql, snakeCaseToCamelCase = TRUE)
  },

  getOutcomeNullDistributions = function(exposureId, analysisId, outcomeType, sourceIds = NULL) {
    sql <- "
    SELECT * FROM @schema.outcome_null_distributions
    WHERE analysis_id = @analysis_id
    AND target_cohort_id = @exposure_id AND outcome_type = @outcome_type
    {@source_ids != ''} ? {AND source_id IN (@source_ids)}"
    queryDb(sql, exposure_id = exposureId, analysis_id = analysisId, outcome_type = outcomeType, source_ids = sourceIds, snakeCaseToCamelCase = TRUE)
  },

  getExposureNullDistributions = function(outcomeId, analysisId, sourceIds = NULL) {
    sql <- "
    SELECT * FROM @schema.exposure_null_distributions
    WHERE analysis_id = @analysis_id AND outcome_cohort_id = @outcome_id
    {@source_ids != ''} ? {AND source_id IN (@source_ids)}"
    queryDb(sql, outcome_id = outcomeId, analysis_id = analysisId, source_ids = sourceIds, snakeCaseToCamelCase = TRUE)
  },


  getExposureOutcomeData = function(exposureId, outcomeId, analysisId = 1, sourceIds = NULL) {
    sql <- "
     SELECT r.SOURCE_ID,
          ds.SOURCE_NAME,
          r.RR,
          r.SE_LOG_RR,
          CONCAT(round(r.LB_95, 2), ' - ', round(r.UB_95, 2)) AS CI_95,
          r.LB_95,
          r.UB_95,
          r.P_VALUE,
          '-' as Calibrated_RR,
          '-' AS CALIBRATED_CI_95,
          '-' as Calibrated_P_VALUE,
          r.C_AT_RISK,
          r.C_PT,
          r.C_CASES,
          r.T_AT_RISK,
          r.T_PT,
          r.T_CASES,
          '-' as I2
      FROM @schema.scc_result r
      INNER JOIN @schema.data_source ds ON ds.source_id = r.source_id
      WHERE r.target_cohort_id = @exposure_id
      AND r.outcome_cohort_id = @outcome_id
      AND r.analysis_id = @analysis_id
      {@source_ids != ''} ? {AND r.SOURCE_ID IN (@source_ids)}
      ORDER BY r.SOURCE_ID
    "
    queryDb(sql, exposure_id = exposureId, outcome_id = outcomeId, analysis_id = analysisId, source_ids = sourceIds)
  },

  getMetaAnalysisTable = function(exposureId, outcomeId, analysisId = 1, calibrationType = 'outcomes', sourceIds = NULL) {
    checkmate::assert_choice(calibrationType, c('outcomes', 'exposures', 'none'))
    rows <- getExposureOutcomeData(exposureId, outcomeId, analysisId, sourceIds = sourceIds)

    if (nrow(rows)) {
      meta <- metaAnalysis(rows)
      meta$CI_95 <- paste(round(meta$LB_95, 2), "-", round(meta$UB_95, 2))

      meta$SOURCE_NAME <- "Meta Analysis"
      meta$CALIBRATED_RR <- "-"
      meta$CALIBRATED_CI_95 <- "-"
      meta$CALIBRATED_P_VALUE <- "-"
      rows <- rbind(rows, meta)
    }

    nullDists <- data.frame()
    if (calibrationType == 'outcomes') {
      outcomeType <- getOutcomeType(outcomeId)
      nullDists <- getOutcomeNullDistributions(exposureId,
                                               analysisId,
                                               ifelse(outcomeType == 3, 2, outcomeType),
                                               sourceIds = sourceIds)
    }

    if (calibrationType == 'exposures') {
      nullDists <- getExposureNullDistributions(outcomeId, analysisId, sourceIds = sourceIds)
    }

    if (nrow(nullDists)) {
      for (i in 1:nrow(nullDists)) {
        nullDist <- nullDists[i,]
        calibratedRow <- getCalibratedValues(rows[rows$SOURCE_ID == nullDist$sourceId,], nullDist)
        ci95 <- paste(round(exp(calibratedRow$logLb95Rr), 2), "-", round(exp(calibratedRow$logUb95Rr), 2))
        rows[rows$SOURCE_ID == nullDist$sourceId,]$CALIBRATED_P_VALUE <- round(calibratedRow$p, 2)
        rows[rows$SOURCE_ID == nullDist$sourceId,]$CALIBRATED_RR <- round(exp(calibratedRow$logRr), 2)
        rows[rows$SOURCE_ID == nullDist$sourceId,]$CALIBRATED_CI_95 <- ci95
      }
    }


    return(rows)
  },

  getCalibratedValues = function(rows, null) {
    # TODO: checknames of inputs
    nullDist <- null$nullDistMean
    nullDist[2] <- null$nullDistSd
    nullDist[3] <- null$sourceId
    names(nullDist) <- c("mean", "sd", "sourceId")
    class(nullDist) <- c("null")
    errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
    ci <- EmpiricalCalibration::calibrateConfidenceInterval(log(rows$RR), rows$SE_LOG_RR, errorModel)
    calibratedPValue <- EmpiricalCalibration::calibrateP(nullDist, log(rows$RR), rows$SE_LOG_RR)
    df <- data.frame(logLb95Rr = ci$logLb95Rr, logUb95Rr = ci$logUb95Rr, p = calibratedPValue, logRr = ci$logRr)
    return(df)
  },

  getForestPlotTable = function(exposureId, outcomeId, calibrated, calibrationType = "outcomes", analysisId = 1, sourceIds = NULL) {
    checkmate::assert_choice(calibrationType, c('outcomes', 'exposures', 'none'))
    baseDf <- getMetaAnalysisTable(exposureId, outcomeId, analysisId = analysisId, calibrationType = 'none', sourceIds = sourceIds)

    baseDf <- baseDf %>% dplyr::arrange(-SOURCE_ID)
    if (!(1 %in% calibrated) | calibrationType == 'none' | nrow(baseDf) == 0) {
      return(baseDf)
    }

    nullDists <- data.frame()
    if (calibrationType == "outcomes") {
      outcomeType <- getOutcomeType(outcomeId)
      nullDists <- getOutcomeNullDistributions(exposureId,
                                               analysisId,
                                               ifelse(outcomeType == 3, 2, outcomeType),
                                               sourceIds = sourceIds)
    }

    if (calibrationType == 'exposures') {
      nullDists <- getExposureNullDistributions(outcomeId, analysisId, sourceIds = sourceIds)
    }

    if (!nrow(nullDists)) {
      return(baseDf)
    }

    calibratedRows <- data.frame()
    for (i in 1:nrow(nullDists)) {
      nullDist <- nullDists[i,]
      row <- baseDf[baseDf$SOURCE_ID == nullDist$sourceId,]
      calibratedDt <- getCalibratedValues(row, nullDist)
      row$RR <- exp(calibratedDt$logRr)
      row$LB_95 <- exp(calibratedDt$logLb95Rr)
      row$UB_95 <- exp(calibratedDt$logUb95Rr)
      row$SOURCE_NAME <- paste(row$SOURCE_NAME, "Calibrated")
      calibratedRows <- rbind(calibratedRows, row)
    }
    calibratedRows <- calibratedRows %>% dplyr::arrange(-SOURCE_ID)

    if (0 %in% calibrated) {
      return(rbind(baseDf, calibratedRows))
    }
    return(calibratedRows)
  },

  getExposureOutcomeDqd = function(exposureOutcomePairs, analysisId = 1) {

    stopifnot("exposureId" %in% colnames(exposureOutcomePairs))
    stopifnot("outcomeId" %in% colnames(exposureOutcomePairs))
    stopifnot(nrow(exposureOutcomePairs) > 0)

    andStrings <- apply(exposureOutcomePairs, 1, function(item) {
      SqlRender::render("(r.target_cohort_id = @exposure_id  AND r.outcome_cohort_id = @outcome_id)",
                        outcome_id = item["outcomeId"],
                        exposure_id = item["exposureId"])
    })
    innerQuery <- paste(andStrings, collapse = " OR ")
    sql <- loadSqlFile("data_quality/getExposureOutcomeDqd.sql")

    queryDb(sql, analysis_id = analysisId, inner_query = innerQuery, snakeCaseToCamelCase = TRUE)
  },

  getOutcomeControls = function(targetIds, sourceIds = NULL, analysisId = 1) {
    sql <- "SELECT * FROM @schema.exposure_cohort_null_data
    WHERE target_cohort_id IN (@exposure_ids)
    AND analysis_id = @analysis_id
    {@source_ids != ''} ? {AND source_id IN (@source_ids)}
    "
    return(queryDb(sql,
                   exposure_ids = targetIds,
                   source_ids = sourceIds,
                   analysis_id = analysisId))
  },

  getExposureControls = function(outcomeIds, sourceIds = NULL, analysisId = 1) {
    sql <- "SELECT * FROM @schema.outcome_cohort_null_data
    WHERE outcome_cohort_id IN (@outcome_ids)
    AND analysis_id = @analysis_id
    {@source_ids != ''} ? {AND source_id IN (@source_ids)}
    "
    return(queryDb(sql,
                   outcome_ids = outcomeIds,
                   source_ids = sourceIds,
                   analysis_id = analysisId))
  },

  getOutcomeType = function(outcomeId) {
    getFirst("SELECT outcome_type FROM @schema.outcome_cohort_definition ocd WHERE ocd.cohort_definition_id = @outcome_id", outcome_id = outcomeId)
  },

  getExposureOutcomeRows = function(exposure, outcome, calibrated) {
    sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRows.sql", package = "rewardb"))
    queryDb(sql, treatment = exposure, outcome = outcome, calibrated = 0, use_calibration = FALSE, result = 'scc_result')
  },

  getOutcomeConceptSet = function(outcomeId) {
    queryDb("SELECT c.concept_name,
             aoc.concept_id,
             include_descendants,
             is_excluded
             from @schema.atlas_outcome_concept aoc
             inner join @vocabulary_schema.concept c on c.concept_id = aoc.concept_id
             WHERE cohort_definition_id = @outcome

             UNION

             SELECT c.concept_name,
             oc.conceptset_id as concept_id,
             1 as include_descendants,
             0 as is_excluded
             from @schema.outcome_cohort_definition oc
             inner join @vocabulary_schema.concept c on c.concept_id = oc.conceptset_id
             WHERE cohort_definition_id = @outcome
             AND conceptset_id != 99999999
             AND outcome_type != 3
             ",
            outcome = outcomeId,
            vocabulary_schema = config$vocabularySchema,
            snakeCaseToCamelCase = TRUE)
  },

  getExposureConceptSet = function(exposureId) {
    queryDb("SELECT c.concept_name,
             c.concept_id,
             include_descendants,
             is_excluded
             from @schema.atlas_exposure_concept aec
             inner join @vocabulary_schema.concept c on c.concept_id = aec.concept_id
             WHERE cohort_definition_id = @exposure

             UNION

             SELECT c.concept_name,
             cd.drug_conceptset_id as concept_id,
             1 as include_descendants,
             0 as is_excluded
             from @schema.cohort_definition cd
             inner join @vocabulary_schema.concept c on c.concept_id = cd.drug_conceptset_id
             WHERE cohort_definition_id = @exposure
             AND atc_flg != -1
             ",
            exposure = exposureId,
            vocabulary_schema = config$vocabularySchema,
            snakeCaseToCamelCase = TRUE)
  },

  getDataSources = function(...) {
    sources <- callSuper(...)
    if(!-99 %in% sources$SOURCE_ID) {
      sources <- rbind(data.frame(SOURCE_ID = -99, SOURCE_NAME = "Meta-analysis"), sources)
    }

    return(sources)
  }
)