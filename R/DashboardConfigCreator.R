makeDashboardYamlConfig <- function(name,
                                    description,
                                    shortName,
                                    dataSources,
                                    filepath,
                                    overwrite = FALSE,
                                    outcomeConceptIds = NULL,
                                    customOutcomeCohortIds = NULL,
                                    exposureCohortIds = NULL,
                                    customExposureIds = NULL) {

  checkmate::assert_string(name)
  checkmate::assert_string(description)
  checkmate::assert_string(dataSources)
  checkmate::assert_string(shortName)
  checkmate::assert_numeric(outcomeConceptIds, null.ok = TRUE)
  checkmate::assert_numeric(exposureCohortIds, null.ok = TRUE)
  checkmate::assert_numeric(customOutcomeCohortIds, null.ok = TRUE)
  checkmate::assert_numeric(customExposureIds, null.ok = TRUE)
  checkmate::assert_access(filepath, "w")

  if (!overwrite && file.exists(filepath)) {
    stop("File already exists. Set overwrite = TRUE to continue")
  }

  if (all(is.null(c(outcomeConceptIds, customOutcomeCohortIds, exposureCohortIds, customExposureIds)))) {
    stop("Must specify outcome or exposurse to generate configuration file")
  }

  if (any(is.numeric(outcomeConceptIds, customOutcomeCohortIds)) &
    any(is.numeric(exposureCohortIds, customExposureIds))) {
    stop("Dashboard configuration must specify outcomes or exposures")
  }

  useExposureControls <- any(is.numeric(outcomeConceptIds, customOutcomeCohortIds))

  config <- list(name = name,
                 description = description,
                 short_name = shortName,
                 outcome_concept_ids = outcomeConceptIds,
                 custom_outcome_cohort_ids = customOutcomeCohortIds,
                 target_cohort_ids = exposureCohortIds,
                 custom_exposure_ids = customExposureIds,
                 useExposureControls = useExposureControls)

  yaml::write_yaml(config, filepath)
}
