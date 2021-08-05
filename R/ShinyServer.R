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
dashboardInstance <- function(input, output, session) {
  library(shiny, warn.conflicts = FALSE)
  library(shinyWidgets, warn.conflicts = FALSE)
  library(scales, warn.conflicts = FALSE)
  library(DT, warn.conflicts = FALSE)
  library(dplyr, warn.conflicts = FALSE)

  getOutcomeCohortTypes <- reactive({
    cohortTypeMapping <- list("ATLAS defined" = 3, "Inpatient" = 0, "Two diagnosis codes" = 1, "One Diagnosis Code" = 2)
    lapply(input$outcomeCohortTypes, function(x) cohortTypeMapping[[x]])
  })

  dataSourceInfo <- model$getDataSourceInfo()
  output$dataSourceTable <- gt::render_gt(dataSourceInfo)

  output$requiredDataSources <- renderUI({
    pickerInput("requiredDataSources",
                label = "Select required data sources for benefit:",
                choices = dataSourceInfo$sourceName,
                options = shinyWidgets::pickerOptions(actionsBox = TRUE),
                multiple = TRUE)
  })

  requiredBenefitSources <- reactive({
    dataSourceInfo[dataSourceInfo$sourceName %in% input$requiredDataSources,]$sourceId
  })

  getMainTableParams <- reactive({
    outcomeCohortNames <- if (length(input$outcomeCohorts)) strQueryWrap(input$outcomeCohorts) else NULL
    targetCohortNames <- if (length(input$targetCohorts)) strQueryWrap(input$targetCohorts) else NULL
    exposureClassNames <- if (appContext$useExposureControls & length(input$exposureClass)) strQueryWrap(input$exposureClass) else NULL
    outcomeTypes <- getOutcomeCohortTypes()

    params <- list(benefitThreshold = input$cutrange1,
                   riskThreshold = input$cutrange2,
                   pValueCut = input$pCut,
                   requiredBenefitSources = requiredBenefitSources(),
                   filterByMeta = input$filterThreshold == "Meta analysis",
                   outcomeCohortTypes = outcomeTypes,
                   excludeIndications = input$excludeIndications,
                   calibrated = input$calibrated,
                   benefitCount = input$scBenefit,
                   riskCount = input$scRisk,
                   outcomeCohortNames = outcomeCohortNames,
                   targetCohortNames = targetCohortNames,
                   exposureClasses = exposureClassNames)

    return(params)
  })

  getMainTableCount <- reactive({
    params <- getMainTableParams()
    res <- do.call(model$getFilteredTableResultsCount, params)
    return(res)
  })

  output$mainTablePage <- renderUI({
    recordCount <- getMainTableCount()
    numPages <- ceiling(recordCount / as.integer(input$mainTablePageSize))
    selectInput("mainTablePage", "Page", choices = 1:numPages)
  })

  getMainTablePage <- reactive({
    return(as.integer(input$mainTablePage))
  })

  output$mainTableNumPages <- renderText({
    recordCount <- getMainTableCount()
    numPages <- ceiling(recordCount / as.integer(input$mainTablePageSize))
    return(paste("Page", getMainTablePage(), "of", numPages))
  })

  output$mainTableCount <- renderText({
    res <- getMainTableCount()
    offset <- max(getMainTablePage() - 1, 0) * as.integer(input$mainTablePageSize) + 1
    endNum <- min(offset + as.integer(input$mainTablePageSize) - 1, res)
    str <- paste("Displaying", offset, "to", endNum, "of", res, "results")
    return(str)
  })

  updateSelectizeInput(session, "outcomeCohorts", choices = model$getOutcomeCohortNames(), server = TRUE)
  updateSelectizeInput(session, "targetCohorts", choices = model$getExposureCohortNames(), server = TRUE)

  if (appContext$useExposureControls) {
    updateSelectizeInput(session, "exposureClass", choices = model$getExposureClassNames(), server = TRUE)
  }

  # Subset of results for harm, risk and treatement categories
  # Logic: either select everything or select a user defined subset
  mainTableReac <- reactive({
    params <- getMainTableParams()
    params$limit <- input$mainTablePageSize
    params$offset <- max(getMainTablePage() - 1, 0) * as.integer(input$mainTablePageSize)
    params$orderByCol <- input$mainTableSortBy
    params$ascending <- input$mainTableOrderAscending
    do.call(model$getFilteredTableResults, params)
  })

  output$mainTable <- DT::renderDataTable({
    df <- mainTableReac()
    tryCatch(
    {
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
    },
      # Handles messy response
      error = function(e) {
        ParallelLogger::logError(paste(e))
        return(data.frame())
      })
  })

  # This links the app components together
  selectedExposureOutcome <- reactive({
    ids <- input$mainTable_rows_selected
    filtered1 <- mainTableReac()

    if (!length(ids)) {
      return(NULL)
    }

    filtered2 <- filtered1[ids,]
    filtered2$calibrationType <- "none"
    return(filtered2)
  })

  fullDataDownload <- reactive({
    model$getFilteredTableResults(benefitThreshold = input$cutrange1,
                                  riskThreshold = input$cutrange2,
                                  pValueCut = input$pCut,
                                  filterByMeta = input$filterThreshold == "Meta analysis",
                                  calibrated = input$calibrated,
                                  benefitCount = input$scBenefit,
                                  riskCount = input$scRisk)
  })

  output$treatmentOutcomeStr <- renderText({
    s <- selectedExposureOutcome()
    return(paste(s$TARGET_COHORT_NAME, s$TARGET_COHORT_ID, "for", s$OUTCOME_COHORT_NAME, s$OUTCOME_COHORT_ID))
  })

  output$downloadData <- downloadHandler(
    filename = function() {
      paste0(appContext$short_name, '-full_results', input$cutrange1, '-', input$cutrange2, '.csv')
    },
    content = function(file) {
      write.csv(fullDataDownload(), file, row.names = FALSE)
    }
  )

  output$downloadFullData <- downloadHandler(
    filename = function() {
      paste0(appContext$short_name, '-export.csv')
    },
    content = function(file) {
      data <- model$getFullDataSet()
      write.csv(data, file, row.names = FALSE)
    }
  )

  getNegativeControls <- reactive({
    model$getNegativeControls()
  })

  output$downloadControls <- downloadHandler(
    filename = function() {
      paste0(appContext$short_name, '-negative-controls.csv')
    },
    content = function(file) {
      write.csv(getNegativeControls(), file, row.names = FALSE)
    }
  )

  getIndications <- reactive({
    model$getMappedAssociations()
  })

  output$downloadIndications <- downloadHandler(
    filename = function() {
      paste0(appContext$short_name, '-indications.csv')
    },
    content = function(file) {
      write.csv(getIndications(), file, row.names = FALSE)
    }
  )

  # Subset without limit
  mainTableDownload <- reactive({
    params <- getMainTableParams()
    do.call(model$getFilteredTableResults, params)
  })

  output$downloadFullTable <- downloadHandler(
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

  # Add cem panel if option is present
  if (!is.null(appContext$cemConnectorApiUrl)) {
    message("loading cem api")
    cemBackend <- CemConnector::CemWebApiBackend$new(apiUrl = appContext$cemConnectorApiUrl)

    ingredientConetpInput <- shiny::reactive({
      selected <- selectedExposureOutcome()
      model$getOutcomeConceptSet(selected$OUTCOME_COHORT_ID)
    })

    conditionConceptInput <- shiny::reactive({
      selected <- selectedExposureOutcome()
      model$getExposureConceptSet(selected$TARGET_COHORT_ID)
    })
    ceModuleServer <- CemConnector::ceExplorerModule("cemExplorer", # This ID should be unqiue and match the call to ceExplorerModuleUi
                                                     cemBackend,
                                                     ingredientConceptInput = ingredientConetpInput,
                                                     conditionConceptInput = conditionConceptInput,
                                                     siblingLookupLevelsInput = shiny::reactive({ 0 }))
    cemPanel <- tabPanel("Evidence", CemConnector::ceExplorerModuleUi("cemExplorer"))
    shiny::appendTab(inputId = "outcomeResultsTabs", cemPanel)

  }
}

#' @title
#' Launch the REWARD Shiny app dashboard
#' @description
#' Launches a Shiny app for a given configuration file
#' @param appConfigPath path to configuration file. This is loaded in to the local environment with the appContext variable
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
