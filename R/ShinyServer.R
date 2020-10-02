#' Requires a server appContext instance to be loaded in environment see scoping of launchDashboard
#' This can be obtained with rewardb::loadAppContext(...)
#' @param input shiny input object
#' @param output shiny output object
#' @param session
serverInstance <- function(input, output, session) {
    library(shiny, warn.conflicts=FALSE)
    library(shinyWidgets, warn.conflicts=FALSE)
    library(scales, warn.conflicts=FALSE)
    library(DT, warn.conflicts=FALSE)
    library(foreach, warn.conflicts=FALSE)

    # Simple wrapper for always ensuring that database connection is opened and closed
    # Postgres + DatabaseConnector has problems with connections hanging around
    dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
    queryDb <- function (query, ...) {
     tryCatch({
        df <- DatabaseConnector::renderTranslateQuerySql(dbConn, query, schema = appContext$short_name, ...)
        return (df)
      },
      error = function(e) {
        ParallelLogger::logError(e)
        DatabaseConnector::disconnect(dbConn)
        dbConn <<- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
      }
     )
    }
    session$onSessionEnded(function() {
        writeLines("Closing connection")
        DatabaseConnector::disconnect(dbConn)
    })

    niceColumnName <- list(
      SOURCE_NAME = "Database",
      RR = "Relative Risk",
      CALIBRATED_RR = "Calibrated Relative Risk",
      C_AT_RISK = "N Unexp",
      T_AT_RISK = "N Exp",
      C_PT = "Unexposed time (years)",
      T_PT = "Exposed time (years)",
      C_CASES = "Unexposed cases",
      T_CASES = "Exposed cases",
      LB_95 = "CI95LB",
      UB_95 = "CI95UB",
      CALIBRATED_LB_95 = "calibrated CI95LB",
      CALIBRATED_UB_95 = "calibrated CI95UB",
      P_VALUE = "P",
      CALIBRATED_P_VALUE = "calibrated P"
    )

    niceColumnNameInv <- list()

    for (n in names(niceColumnName)) {
        niceColumnNameInv[niceColumnName[[n]]] <- n
    }
    getOutcomeCohortTypes <- reactive(
        {
            cohortTypeMapping <- list( "ATLAS defined" = 2, "Inpatient" = 1, "Two diagnosis codes" = 0)
            rs <- foreach(i=input$outcomeCohortTypes) %do% { cohortTypeMapping[[i]] }
            return(rs)
        }
    )
    # Query full results, only filter is Risk range parameters
    mainTableRe <- reactive({
        benefit <- input$cutrange1
        risk <- input$cutrange2
        pCut <- input$pCut
        filterByMeta <- input$filterThreshold == "Meta analysis"
        outcomeCohortTypes <- getOutcomeCohortTypes()
        mainTableSql <- readr::read_file(system.file("sql/queries/", "mainTable.sql", package = "rewardb"))
        calibrated <- ifelse(input$calibrated, 1, 0)
        bSelection <- paste0("'", paste0(input$scBenefit, sep="'"))
        rSelection <- paste0("'", paste0(input$scRisk, sep="'"))
        df <- queryDb(
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

    df <- queryDb("SELECT DISTINCT COHORT_NAME FROM @schema.OUTCOME ORDER BY COHORT_NAME")
    updateSelectizeInput(session, "outcomeCohorts", choices = df$COHORT_NAME, server = TRUE)

    df <- queryDb("SELECT DISTINCT COHORT_NAME FROM @schema.TARGET ORDER BY COHORT_NAME")
    updateSelectizeInput(session, "targetCohorts", choices = df$COHORT_NAME, server = TRUE)

    if (appContext$useExposureControls) {
        df <- queryDb("SELECT DISTINCT EXPOSURE_CLASS_NAME FROM @schema.EXPOSURE_CLASS ORDER BY EXPOSURE_CLASS_NAME")
        updateSelectizeInput(session, "exposureClass", choices = df$EXPOSURE_CLASS_NAME, server = TRUE)
    }

    # Subset of results for harm, risk and treatement categories
    # Logic: either select everything or select a user defined subset
    mainTableRiskHarmFilters <- reactive({
        filtered <- mainTableRe()
        if (length(input$outcomeCohorts)) {
            filtered <- filtered[filtered$OUTCOME_COHORT_NAME %in% input$outcomeCohorts, ]
        }

        if (length(input$targetCohorts)) {
            filtered <- filtered[filtered$TARGET_COHORT_NAME %in% input$targetCohorts, ]
        }

        if (appContext$useExposureControls & length(input$exposureClass)) {
            filtered <- filtered[filtered$ECN %in% input$exposureClass, ]
        }

        return(filtered)
    })

    output$mainTable <- DT::renderDataTable({
        df <- mainTableRiskHarmFilters()
        tryCatch(
          {
          if(length(df$I2)) {
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

    filteredTableSelected <- reactive({
        ids <- input$mainTable_rows_selected
        filtered1 <- mainTableRiskHarmFilters()
        filtered2 <- filtered1[ids, ]
        return(filtered2)
    })

    output$treatmentOutcomeStr <- renderText({
        s <- filteredTableSelected()
        return(paste(s$TARGET_COHORT_NAME, s$TARGET_COHORT_ID, "for", s$OUTCOME_COHORT_NAME, s$OUTCOME_COHORT_ID))
    })

    output$targetStr <- renderText({
        s <- filteredTableSelected()
        s$TARGET_COHORT_NAME
    })

    metaAnalysisTbl <- reactive({
        s <- filteredTableSelected()
        treatment <- s$TARGET_COHORT_ID
        outcome <- s$OUTCOME_COHORT_ID
        if (length(outcome)) {
            updateTabsetPanel(session, "mainPanel", "Detail")
            sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRowsGrouped.sql", package = "rewardb"))
            table <- queryDb(sql, treatment = treatment, outcome = outcome)
            return(table)
        }
        return(data.frame())
    })

    fullResultsTable <- reactive({
        option = list(columnDefs = list(list(targets = c(8, 11), class = "dt-right")))
        table3 <- metaAnalysisTbl()
        if(nrow(table3) >= 1) {
            table3$RR[table3$RR > 100] <- NA
            table3$C_PT <- formatC(table3$C_PT, digits = 0, format = "f")
            table3$T_PT <- formatC(table3$T_PT, digits = 0, format = "f")
            table3$RR <- formatC(table3$RR, digits = 2, format = "f")
            table3$LB_95 <- formatC(table3$LB_95, digits = 2, format = "f")
            table3$UB_95 <- formatC(table3$UB_95, digits = 2, format = "f")
            table3$P_VALUE <- formatC(table3$P_VALUE, digits = 2, format = "f")

            table3$CALIBRATED_RR <- formatC(table3$CALIBRATED_RR, digits = 2, format = "f")
            table3$CALIBRATED_LB_95 <- formatC(table3$CALIBRATED_LB_95, digits = 2, format = "f")
            table3$CALIBRATED_UB_95 <- formatC(table3$CALIBRATED_UB_95, digits = 2, format = "f")
            table3$CALIBRATED_P_VALUE <- formatC(table3$CALIBRATED_P_VALUE, digits = 2, format = "f")

            for (n in names(niceColumnName)) {
                colnames(table3)[colnames(table3) == n] <- niceColumnName[n]
            }

            headers <- names(niceColumnNameInv)
            table4 <- DT::datatable(table3[, headers], rownames = FALSE, escape = FALSE, )
            return(table4)
        }
    })

    fullDataDownload <- reactive({
        benefit <- input$cutrange1
        risk <- input$cutrange2
        mainTableSql <- readr::read_file(system.file("sql/queries/", "fullResultsTable.sql", package = "rewardb"))
        calibrated <- ifelse(input$calibrated, 1, 0)
        bSelection <- paste0("'", paste0(input$scBenefit, sep="'"))
        rSelection <- paste0("'", paste0(input$scRisk, sep="'"))
        df <- queryDb(mainTableSql, risk = risk, benefit = benefit,
                      risk_selection = rSelection, benefit_selection = bSelection, calibrated=calibrated)
        return(df)
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
      df <- queryDb(sql)
      return(df)
    })

    output$downloadControls <- downloadHandler(
      filename = function()  {
        paste0(appContext$short_name, '-negative-controls.csv')
      },
      content = function(file) {
        write.csv(getNegativeControls(), file, row.names = FALSE)
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

    output$downloadSubTable <- downloadHandler(
      filename = function() {
        s <- filteredTableSelected()
        treatment <- s$TARGET_COHORT_ID
        outcome <- s$OUTCOME_COHORT_ID
        paste0(appContext$short_name, '-results-', treatment, "-", outcome, '.csv')
      },
      content = function(file) {
        write.csv(metaAnalysisTbl(), file, row.names = FALSE)
      }
    )

    output$fullResultsTable <- DT::renderDataTable(
      expr = {
          tryCatch(
            expr = {
                return(fullResultsTable())
            },
            error = function(e) {
                return(data.frame())
            })
      }
    )

    forestPlotTable <- reactive({
        s <- filteredTableSelected()
        treatment <- s$TARGET_COHORT_ID
        outcome <- s$OUTCOME_COHORT_ID
        if (length(outcome)) {
            updateTabsetPanel(session, "mainPanel", "Detail")
            sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRows.sql", package = "rewardb"))
            uncalibratedTable <- queryDb(sql, treatment = treatment, outcome = outcome, calibrated=0)
            calibratedTable <- queryDb(sql, treatment = treatment, outcome = outcome, calibrated=1)

            if (nrow(calibratedTable)) {
                calibratedTable$calibrated = "Calibrated"
                uncalibratedTable$calibrated = "Uncalibrated"
                uncalibratedTable$SOURCE_NAME <- paste0(uncalibratedTable$SOURCE_NAME, "\n uncalibrated")
                calibratedTable$SOURCE_NAME <- paste0(calibratedTable$SOURCE_NAME, "\n Calibrated")
                table <- rbind(uncalibratedTable[order(uncalibratedTable$SOURCE_ID, decreasing = TRUE), ],
                               calibratedTable[order(calibratedTable$SOURCE_ID, decreasing = TRUE), ])
                return(table)
            }
            return(uncalibratedTable)
        }
        return(data.frame())
    })

    output$forestPlot <- plotly::renderPlotly({
        df <- forestPlotTable()
        if (nrow(df)) {
            return(plotly::ggplotly(rewardb::forestPlot(df)))
        }
    })

    output$downloadForestPlot <- downloadHandler(
      filename = function() {
        s <- filteredTableSelected()
        treatment <- s$TARGET_COHORT_ID
        outcome <- s$OUTCOME_COHORT_ID
        paste0(appContext$short_name, '-forest-plot-', treatment, "-", outcome, '.png')
      },
      content = function(file) {
        df <- forestPlotTable()
        ggplot2::ggsave(file, plot = rewardb::forestPlot(df), device = "png")
      }
    )

    getCalibrationPlot <- function() {
      s <- filteredTableSelected()
      treatment <- s$TARGET_COHORT_ID
      outcome <- s$OUTCOME_COHORT_ID
      sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRows.sql", package = "rewardb"))
      positives <- queryDb(sql, treatment = treatment, outcome = outcome, calibrated=0)

      if (appContext$useExposureControls) {
          message("getting target control results")
          negatives <- rewardb::getExposureControls(appContext, dbConn, outcomeCohortIds=outcome)
      } else {
          message(paste("getting outcome controls results", treatment))
          negatives <- rewardb::getOutcomeControls(appContext, dbConn, targetIds=treatment)
      }

      plot <- EmpiricalCalibration::plotCalibrationEffect(
        logRrNegatives = log(negatives$RR),
        seLogRrNegatives = negatives$SE_LOG_RR,
        logRrPositives = log(positives$RR),
        seLogRrPositives = positives$SE_LOG_RR
      )
    }

    getNullDist <- function () {
      s <- filteredTableSelected()
      treatment <- s$TARGET_COHORT_ID
      outcome <- s$OUTCOME_COHORT_ID
      if (appContext$useExposureControls) {                                      
          negatives <- rewardb::getExposureControls(appContext, dbConn, outcomeCohortIds=outcome)
      } else {                                                                   
          negatives <- rewardb::getOutcomeControls(appContext, dbConn, targetIds=treatment)
      }                                                                          
      
      null <- EmpiricalCalibration::fitNull(log(negatives$RR), negatives$SE_LOG_RR)
    }

    output$calibrationPlot <- plotly::renderPlotly({
        plot <- getCalibrationPlot()
        return(plotly::ggplotly(plot))
    })

    output$nullDistribution <- shiny::renderText({
      null <- getNullDist()
      return(
        paste(
          "Null distribution mean:", round(exp(null["mean"]), 3),
          "std:", round(exp(null["sd"]), 3)
        )
      )
    })

    output$downloadCalibrationPlot <- downloadHandler(
      filename = function() {
        s <- filteredTableSelected()
        treatment <- s$TARGET_COHORT_ID
        outcome <- s$OUTCOME_COHORT_ID
        paste0(appContext$short_name, '-calibration-plot-', treatment, "-", outcome, '.png')
      },
      content = function(file) {
        ggplot2::ggsave(file, plot = getCalibrationPlot(), device = "png")
      }
    )
}

#' Launch the REWARD-B Shiny app
#' @param configPath path to configuration file. This is loaded in to the local environment with the appContext variable
#' @details
#' Launches a Shiny app for a given configuration file
#' @export
launchDashboard <- function (configPath) {
  e <- environment()
  e$appContext <- rewardb::loadAppContext(configPath)
  shiny::shinyApp(server=serverInstance, dashboardUi, enableBookmarking = "server")
}
