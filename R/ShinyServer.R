strQueryWrap <- function(vec) {
  vec <- gsub("'", "''", vec)
  paste0("'", vec, "'", sep = "")
}

#' Wrapper around boxplot module
timeOnTreatmentServer <- function(id, model, selectedExposureOutcome) {
  caption <- "Table: Shows time on treatment for population od patients exposed to medication that experience the outcome of interest."
  server <- shiny::moduleServer(id, boxPlotModuleServer(model$getTimeOnTreatmentStats, caption, selectedExposureOutcome))
  return(server)
}

#' Wrapper around boxplot module
timeToOutcomeServer <- function(id, model, selectedExposureOutcome) {
  caption <- "Table: shows distribution of absolute difference of time between exposure and outcome for population of patients exposed to medication that expeirence the outcome."
  server <- shiny::moduleServer(id, boxPlotModuleServer(model$getTimeToOutcomeStats, caption, selectedExposureOutcome))
  return(server)
}

#' @title
#' Dashboard instance
#' @description
#' Requires a server appContext instance to be loaded in environment see scoping of launchDashboard
#' This can be obtained with rewardb::loadAppContext(...)
#' @param input shiny input object
#' @param output shiny output object
#' @param session shiny session
#' @importFrom gt render_gt
#' @import shiny
dashboardInstance <- function(input, output, session) {
  dataSourceInfo <- shiny::reactive({ model$getDataSourceInfo() })
  output$dataSourceTable <- gt::render_gt(dataSourceInfo())

  output$requiredDataSources <- shiny::renderUI({
    shinyWidgets::pickerInput("requiredDataSources",
                              label = "Select required data sources for benefit:",
                              choices = dataSourceInfo()$sourceName,
                              options = shinyWidgets::pickerOptions(actionsBox = TRUE),
                              multiple = TRUE)
  })

  requiredBenefitSources <- shiny::reactive({
    dsi <- dataSourceInfo()
    dsi[dsi$sourceName %in% input$requiredDataSources,]$sourceId
  })

  getMainTableParams <- shiny::reactive({
    outcomeCohortNames <- if (length(input$outcomeCohorts)) strQueryWrap(input$outcomeCohorts) else NULL
    targetCohortNames <- if (length(input$targetCohorts)) strQueryWrap(input$targetCohorts) else NULL
    exposureClassNames <- if (appContext$useExposureControls & length(input$exposureClass)) strQueryWrap(input$exposureClass) else NULL
    outcomeTypes <- input$outcomeCohortTypes

    params <- list(benefitThreshold = input$cutrange1,
                   riskThreshold = input$cutrange2,
                   pValueCut = input$pCut,
                   requiredBenefitSources = requiredBenefitSources(),
                   filterByMeta = input$filterThreshold == "Meta analysis",
                   outcomeCohortTypes = outcomeTypes,
                   calibrated = input$calibrated,
                   benefitCount = input$scBenefit,
                   riskCount = input$scRisk,
                   outcomeCohortNames = outcomeCohortNames,
                   targetCohortNames = targetCohortNames,
                   exposureClasses = exposureClassNames)

    return(params)
  })

  getMainTableCount <- shiny::reactive({
    params <- getMainTableParams()
    res <- do.call(model$getFilteredTableResultsCount, params)
    return(res)
  })

  mainTablePage <- shiny::reactiveVal(1)
  mainTableMaxPages <- shiny::reactive({
    recordCount <- getMainTableCount()
    ceiling(recordCount / as.integer(input$mainTablePageSize))
  })

  shiny::observeEvent(input$mainTableNext, {
    mainTablePage <- mainTablePage() + 1
    if (mainTablePage <= mainTableMaxPages()) {
      mainTablePage(mainTablePage)
    }

  })
  shiny::observeEvent(input$mainTablePrevious, {
    mainTablePage <- mainTablePage() - 1
    if (mainTablePage > 0) {
      mainTablePage(mainTablePage)
    }
  })

  getMainTablePage <- shiny::reactive({
    return(mainTablePage())
  })

  output$mainTablePage <- shiny::renderUI({

    numPages <- mainTableMaxPages()
    shiny::selectInput("mainTablePage", "Page", choices = 1:numPages, selected = mainTablePage())
  })

  shiny::observeEvent(input$mainTablePage, {
    mainTablePage(as.integer(input$mainTablePage))
  })

  output$mainTableNumPages <- shiny::renderText({
    recordCount <- getMainTableCount()
    numPages <- ceiling(recordCount / as.integer(input$mainTablePageSize))
    return(paste("Page", getMainTablePage(), "of", numPages))
  })

  output$mainTableCount <- shiny::renderText({
    res <- getMainTableCount()
    offset <- max(getMainTablePage() - 1, 0) * as.integer(input$mainTablePageSize) + 1
    endNum <- min(offset + as.integer(input$mainTablePageSize) - 1, res)
    str <- paste("Displaying", offset, "to", endNum, "of", res, "results")
    return(str)
  })

  shiny::updateSelectizeInput(session, "outcomeCohorts", choices = model$getOutcomeCohortNames(), server = TRUE)
  shiny::updateSelectizeInput(session, "targetCohorts", choices = model$getExposureCohortNames(), server = TRUE)

  if (appContext$useExposureControls) {
    shiny::updateSelectizeInput(session, "exposureClass", choices = model$getExposureClassNames(), server = TRUE)
  }

  # Subset of results for harm, risk and treatement categories
  # Logic: either select everything or select a user defined subset
  mainTableReac <- shiny::reactive({
    params <- getMainTableParams()
    params$limit <- input$mainTablePageSize
    params$offset <- max(getMainTablePage() - 1, 0) * as.integer(input$mainTablePageSize)
    params$orderByCol <- input$mainTableSortBy
    params$ascending <- input$mainTableOrderAscending
    do.call(model$getFilteredTableResults, params)
  })

  output$mainTable <- DT::renderDataTable({
    df <- mainTableReac()
    if (length(df$I2)) {
      df$I2 <- formatC(df$I2, digits = 2, format = "f")
    }
    colnames(df)[colnames(df) == "I2"] <- "I-squared"
    colnames(df)[colnames(df) == "META_RR"] <- "IRR (meta analysis)"
    colnames(df)[colnames(df) == "RISK_COUNT"] <- "Sources with scc risk"
    colnames(df)[colnames(df) == "BENEFIT_COUNT"] <- "Sources with scc benefit"
    colnames(df)[colnames(df) == "OUTCOME_COHORT_NAME"] <- "Outcome"
    colnames(df)[colnames(df) == "TARGET_COHORT_NAME"] <- "Exposure"
    colnames(df)[colnames(df) == "TARGET_COHORT_ID"] <- "Target cohort id"
    colnames(df)[colnames(df) == "OUTCOME_COHORT_ID"] <- "Outcome cohort id"

    if (appContext$useExposureControls) {
      colnames(df)[colnames(df) == "ECN"] <- "ATC 3"
    }
    table <- DT::datatable(
      df, selection = "single", options = list(dom = 't', pageLength = input$mainTablePageSize, ordering = F),
      rownames = FALSE
    )
    return(table)
  })

  # This links the app components together
  selectedExposureOutcome <- shiny::reactive({
    ids <- input$mainTable_rows_selected
    filtered1 <- mainTableReac()

    if (!length(ids)) {
      return(NULL)
    }

    filtered2 <- filtered1[ids,]
    filtered2$calibrationType <- "none"
    return(filtered2)
  })

  fullDataDownload <- shiny::reactive({
    model$getFilteredTableResults(benefitThreshold = input$cutrange1,
                                  riskThreshold = input$cutrange2,
                                  pValueCut = input$pCut,
                                  filterByMeta = input$filterThreshold == "Meta analysis",
                                  calibrated = input$calibrated,
                                  benefitCount = input$scBenefit,
                                  riskCount = input$scRisk)
  })

  output$treatmentOutcomeStr <- shiny::renderText({
    s <- selectedExposureOutcome()
    return(paste(s$TARGET_COHORT_NAME, "for", s$OUTCOME_COHORT_NAME))
  })

  output$downloadData <- shiny::downloadHandler(
    filename = function() {
      paste0(appContext$short_name, '-full_results', input$cutrange1, '-', input$cutrange2, '.csv')
    },
    content = function(file) {
      write.csv(fullDataDownload(), file, row.names = FALSE)
    }
  )

  output$downloadFullData <- shiny::downloadHandler(
    filename = function() {
      paste0(appContext$short_name, '-export.csv')
    },
    content = function(file) {
      data <- model$getFullDataSet()
      write.csv(data, file, row.names = FALSE)
    })

  getNegativeControls <- shiny::reactive({
    model$getNegativeControls()
  })

  output$downloadControls <- downloadHandler(
    filename = function() {
      paste0(appContext$short_name, '-negative-controls.csv')
    },
    content = function(file) {
      write.csv(getNegativeControls(), file, row.names = FALSE)
    })

  getIndications <- shiny::reactive({
    model$getMappedAssociations()
  })

  output$downloadIndications <- shiny::downloadHandler(
    filename = function() {
      paste0(appContext$short_name, '-indications.csv')
    },
    content = function(file) {
      write.csv(getIndications(), file, row.names = FALSE)
    }
  )

  # Subset without limit
  mainTableDownload <- shiny::reactive({
    params <- getMainTableParams()
    do.call(model$getFilteredTableResults, params)
  })

  output$downloadFullTable <- shiny::downloadHandler(
    filename = function() {
      paste0(appContext$short_name, '-filtered-', input$cutrange1, '-', input$cutrange2, '.csv')
    },
    content = function(file) {
      write.csv(mainTableDownload(), file, row.names = FALSE)
    }
  )

  metaAnalysisTableServer("metaTable", model, selectedExposureOutcome)
  forestPlotServer("forestPlot", model, selectedExposureOutcome)
  calibrationPlotServer("calibrationPlot", model, selectedExposureOutcome, useExposureControls = model$config$useExposureControls)

  timeOnTreatmentServer("timeOnTreatment", model, selectedExposureOutcome)
  tabPanelTimeOnTreatment <- tabPanel("Time on treatment", boxPlotModuleUi("timeOnTreatment"))
  shiny::appendTab(inputId = "outcomeResultsTabs", tabPanelTimeOnTreatment)

  timeToOutcomeServer("timeToOutcome", model, selectedExposureOutcome)
  tabPanelTimeToOutcome <- tabPanel("Time to outcome", boxPlotModuleUi("timeToOutcome"))
  shiny::appendTab(inputId = "outcomeResultsTabs", tabPanelTimeToOutcome)

  ingredientConetpInput <- shiny::reactive({
    selected <- selectedExposureOutcome()
    if (is.null(selected))
      return(data.frame())
    model$getExposureConceptSet(selected$TARGET_COHORT_ID)
  })

  conditionConceptInput <- shiny::reactive({
    selected <- selectedExposureOutcome()
    if (is.null(selected))
      return(data.frame())
    model$getOutcomeConceptSet(selected$OUTCOME_COHORT_ID)
  })


  output$selectedOutcomeConceptSet <- DT::renderDataTable({ conditionConceptInput() })
  output$selectedExposureConceptSet <- DT::renderDataTable({ ingredientConetpInput() })

  # Add cem panel if option is present
  if (!is.null(appContext$cemConnectionDetails)) {
    message("loading cem api")
    cemBackend <- do.call(CemConnector::createCemConnection, appContext$cemConnectionDetails)
    ceModuleServer <- CemConnector::ceExplorerModule("cemExplorer",
                                                     cemBackend,
                                                     ingredientConceptInput = ingredientConetpInput,
                                                     conditionConceptInput = conditionConceptInput,
                                                     siblingLookupLevelsInput = shiny::reactive({ 0 }))
    cemPanel <- shiny::tabPanel("Evidence", CemConnector::ceExplorerModuleUi("cemExplorer"))
    shiny::appendTab(inputId = "outcomeResultsTabs", cemPanel)
  }
}

#' @title
#' Launch the REWARD Shiny app dashboard
#' @description
#' Launches a Shiny app for a given configuration file
#' @param appConfigPath path to configuration file. This is loaded in to the local environment with the appContext variable
#'
#' @import shiny
#' @export
launchDashboard <- function(appConfigPath, globalConfigPath) {
  .GlobalEnv$appContext <- loadShinyAppContext(appConfigPath, globalConfigPath)
  .GlobalEnv$model <- DashboardDbModel(appContext)
  shiny::shinyApp(server = dashboardInstance, dashboardUi, enableBookmarking = "url", onStart = function() {
    shiny::onStop(function() {
      writeLines("Closing connection")
      model$closeConnection()
    })
  })
}
