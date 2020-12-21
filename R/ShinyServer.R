#' Requires a server appContext instance to be loaded in environment see scoping of launchDashboard
#' This can be obtained with rewardb::loadAppContext(...)
#' @param input shiny input object
#' @param output shiny output object
#' @param session
serverInstance <- function(input, output, session) {
  library(shiny, warn.conflicts = FALSE)
  library(shinyWidgets, warn.conflicts = FALSE)
  library(scales, warn.conflicts = FALSE)
  library(DT, warn.conflicts = FALSE)
  library(foreach, warn.conflicts = FALSE)
  library(dplyr, warn.conflicts = FALSE)

  model <- DbModel(appContext)

  session$onSessionEnded(function() {
    writeLines("Closing connection")
    model$exit()
    rm(model)
  })

  getOutcomeCohortTypes <- reactive({
    cohortTypeMapping <- list("ATLAS defined" = 2, "Inpatient" = 1, "Two diagnosis codes" = 0)
    rs <- foreach(i = input$outcomeCohortTypes) %do% { cohortTypeMapping[[i]] }
    return(rs)
  })

  # Query full results, only filter is Risk range parameters
  mainTableRe <- reactive({
    benefit <- input$cutrange1
    risk <- input$cutrange2
    pCut <- input$pCut
    filterByMeta <- input$filterThreshold == "Meta analysis"
    outcomeCohortTypes <- getOutcomeCohortTypes()
    mainTableSql <- readr::read_file(system.file("sql/queries/", "mainTable.sql", package = "rewardb"))
    calibrated <- ifelse(input$calibrated, 1, 0)
    bSelection <- paste0("'", paste0(input$scBenefit, sep = "'"))
    rSelection <- paste0("'", paste0(input$scRisk, sep = "'"))
    df <- model$queryDb(
      mainTableSql,
      risk = risk,
      benefit = benefit,
      p_cut_value = pCut,
      exclude_indications = input$excludeIndications,
      filter_outcome_types = length(outcomeCohortTypes) > 0,
      outcome_types = outcomeCohortTypes,
      risk_selection = rSelection,
      benefit_selection = bSelection,
      calibrated = calibrated,
      show_exposure_classes = appContext$useExposureControls,
      filter_by_meta_analysis = filterByMeta
    )
    return(df)
  })

  df <- model$queryDb("SELECT DISTINCT COHORT_NAME FROM @schema.outcome ORDER BY COHORT_NAME")
  updateSelectizeInput(session, "outcomeCohorts", choices = df$COHORT_NAME, server = TRUE)

  df <- model$queryDb("SELECT DISTINCT COHORT_NAME FROM @schema.target ORDER BY COHORT_NAME")
  updateSelectizeInput(session, "targetCohorts", choices = df$COHORT_NAME, server = TRUE)

  if (appContext$useExposureControls) {
    df <- model$queryDb("SELECT DISTINCT EXPOSURE_CLASS_NAME FROM @schema.exposure_class ORDER BY EXPOSURE_CLASS_NAME")
    updateSelectizeInput(session, "exposureClass", choices = df$EXPOSURE_CLASS_NAME, server = TRUE)
  }

  # Subset of results for harm, risk and treatement categories
  # Logic: either select everything or select a user defined subset
  mainTableRiskHarmFilters <- reactive({
    filtered <- mainTableRe()
    if (length(input$outcomeCohorts)) {
      filtered <- filtered[filtered$OUTCOME_COHORT_NAME %in% input$outcomeCohorts,]
    }

    if (length(input$targetCohorts)) {
      filtered <- filtered[filtered$TARGET_COHORT_NAME %in% input$targetCohorts,]
    }

    if (appContext$useExposureControls & length(input$exposureClass)) {
      filtered <- filtered[filtered$ECN %in% input$exposureClass,]
    }

    return(filtered)
  })

  output$mainTable <- DT::renderDataTable({
    df <- mainTableRiskHarmFilters()
    tryCatch(
    {
      if (length(df$I2)) {
        df$I2 <- formatC(df$I2, digits = 2, format = "f")
      }
      colnames(df)[colnames(df) == "I2"] <- "I-squared"
      colnames(df)[colnames(df) == "META_RR"] <- "IRR (meta analysis)"
      colnames(df)[colnames(df) == "RISK_COUNT"] <- "Sources with scc risk"
      colnames(df)[colnames(df) == "BENEFIT_COUNT"] <- "Sources with scc benefit"
      colnames(df)[colnames(df) == "OUTCOME_COHORT_NAME"] <- "Outcome cohort name"
      colnames(df)[colnames(df) == "TARGET_COHORT_NAME"] <- "Exposure"
      colnames(df)[colnames(df) == "TARGET_COHORT_ID"] <- "Target cohort id"
      colnames(df)[colnames(df) == "OUTCOME_COHORT_ID"] <- "Outcome cohort id"

      if (appContext$useExposureControls) {
        colnames(df)[colnames(df) == "ECN"] <- "ATC 3"
      }
      table <- DT::datatable(
        df, selection = "single",
        rownames = FALSE
      )
      return(table)
    },
    # Handles messy response
      error = function(e) {
        ParallelLogger::logError(paste(e))
        return(DT::datatable(data.frame()))
      })
  })

  selectedExposureOutcome <- reactive({
    ids <- input$mainTable_rows_selected # This links the app components together
    filtered1 <- mainTableRiskHarmFilters()
    filtered2 <- filtered1[ids,]
    return(filtered2)
  })

  fullDataDownload <- reactive({
    benefit <- input$cutrange1
    risk <- input$cutrange2
    mainTableSql <- readr::read_file(system.file("sql/queries/", "fullResultsTable.sql", package = "rewardb"))
    calibrated <- ifelse(input$calibrated, 1, 0)
    bSelection <- paste0("'", paste0(input$scBenefit, sep = "'"))
    rSelection <- paste0("'", paste0(input$scRisk, sep = "'"))
    df <- model$queryDb(mainTableSql, risk = risk, benefit = benefit,
                        risk_selection = rSelection, benefit_selection = bSelection, calibrated = calibrated)
    return(df)
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

  getNegativeControls <- reactive({
    sql <- readr::read_file(system.file("sql/export/", "negativeControls.sql", package = "rewardb"))
    df <- model$queryDb(sql)
    return(df)
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
    sql <- readr::read_file(system.file("sql/export/", "mappedIndications.sql", package = "rewardb"))
    df <- model$queryDb(sql)
    return(df)
  })

  output$downloadIndications <- downloadHandler(
    filename = function() {
      paste0(appContext$short_name, '-indications.csv')
    },
    content = function(file) {
      write.csv(getIndications(), file, row.names = FALSE)
    }
  )

  output$downloadFullTable <- downloadHandler(
    filename = function() {
      paste0(appContext$short_name, '-filtered-', input$cutrange1, '-', input$cutrange2, '.csv')
    },
    content = function(file) {
      write.csv(mainTableRiskHarmFilters(), file, row.names = FALSE)
    }
  )

  metaAnalysisTableServer("metaTable", model, selectedExposureOutcome)
  forestPlotServer("forestPlot", model, selectedExposureOutcome)
  calibrationPlotServer("calibrationPlot", model, selectedExposureOutcome)

  if (model$tableExists("time_on_treatment")) {
    timeOnTreatmentServer("timeOnTreatment", model, selectedExposureOutcome)
    tabPanelTimeOnTreatment <- tabPanel("Time on treatment", timeOnTreatmentUi("timeOnTreatment"))
    shiny::appendTab(inputId = "outcomeResultsTabs", tabPanelTimeOnTreatment)

    timeToOutcomeServer("timeToOutcome", model, selectedExposureOutcome)
    tabPanelTimeToOutcome <- tabPanel("Time to outcome", timeToOutcomeUi("timeToOutcome"))
    shiny::appendTab(inputId = "outcomeResultsTabs", tabPanelTimeToOutcome)
  }
}

#' Launch the REWARD-B Shiny app
#' @param appConfigPath path to configuration file. This is loaded in to the local environment with the appContext variable
#' @details
#' Launches a Shiny app for a given configuration file
#' @export
launchDashboard <- function(appConfigPath, globalConfigPath) {
  e <- environment()
  e$appContext <- loadAppContext(appConfigPath, globalConfigPath)
  shiny::shinyApp(server = serverInstance, dashboardUi, enableBookmarking = "url")
}
