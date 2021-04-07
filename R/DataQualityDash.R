dataQualityInstance <- function(input, output, session) {
  library(shiny, warn.conflicts = FALSE)
  library(shinycssloaders, warn.conflicts = FALSE)
  library(gt)
  exposureOutcomePairs <- data.frame(
    exposureId = c(7869, 7869),
    outcomeId = c(311525, 311526)
  )

  baseData <- model$getExposureOutcomeDqd(exposureOutcomePairs) %>%
    gt() %>%
    fmt_number(decimals = 3, columns = c(3, 4, 6, 7)) %>%
    tab_options(
      table.font.size = "tiny"
    )
  output$expousreOutcomeTable <- gt::render_gt(baseData)

  dataSourceInfo <- model$queryDb(" SELECT * from @schema.data_source", snakeCaseToCamelCase = TRUE)
  output$dataSourceTable <- gt::render_gt(dataSourceInfo)

}

dqdUi <- function() {
  library(shiny, warn.conflicts = FALSE)
  library(shinycssloaders, warn.conflicts = FALSE)

  fluidPage(
    title = "REWARD data quality dashboard",
    h2("Registerd CDMs"),
    withSpinner(gt::gt_output(outputId = "dataSourceTable")),
    h2("Exposure outcome cohorts"),
    withSpinner(gt::gt_output(outputId = "expousreOutcomeTable"))
  )
}

launchDataQualityDashboard <- function(globalConfigPath) {
  .GlobalEnv$reportAppContext <- loadReportContext(globalConfigPath)
  .GlobalEnv$model <- ReportDbModel(reportAppContext)
  shiny::shinyApp(server = dataQualityInstance, dqdUi(), enableBookmarking = "url", onStart = function() {
    shiny::onStop(function() {
      writeLines("Closing connection")
      model$closeConnection()
    })
  })
}
