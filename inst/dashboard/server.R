library(shiny)
library(shinyWidgets)
library(scales)
library(DT)
library(rewardb)

server <- function(input, output, session) {
    # Simple wrapper for always ensuring that database connection is opened and closed
    # Postgres + DatabaseConnector has problems with connections hanging around
    queryDb <- function (query, ...) {
        dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
        df <- DatabaseConnector::renderTranslateQuerySql(dbConn, query, schema = appContext$short_name, ...)
        DatabaseConnector::disconnect(dbConn)
        return (df)
    }

    niceColumnName <- list(SOURCE_NAME = "Database", RR = "Relative Risk", C_AT_RISK = "N Unexposed", T_AT_RISK = "N Exposed",
    C_PT = "Unexposed time (years)", T_PT = "Exposed time (years)", C_CASES = "Unexposed cases", T_CASES = "Exposed cases",
    LB_95 = "CI95LB", UB_95 = "CI95UB", P_VALUE = "P", I2 = "I-square")

    niceColumnNameInv <- list()

    for (n in names(niceColumnName)) {
        niceColumnNameInv[niceColumnName[[n]]] <- n
    }


    # Query full results, only filter is Risk range parameters
    mainTableRe <- reactive({
        benefit <- input$cutrange1
        risk <- input$cutrange2
        mainTableSql <- readr::read_file(system.file("sql/queries/", "mainTable.sql", package = "rewardb"))
        calibrated <- ifelse(input$calibrated, 1, 0)
        bSelection <- paste0("'", paste0(input$scBenefit, sep="'"))
        rSelection <- paste0("'", paste0(input$scRisk, sep="'"))
        df <- queryDb(mainTableSql, risk = risk, benefit = benefit,
                      risk_selection = rSelection, benefit_selection = bSelection, calibrated=calibrated)

        return(df)
    })

    allOutcomeCohortNames <- reactive({ queryDb("SELECT DISTINCT COHORT_NAME FROM @schema.OUTCOME ORDER BY COHORT_NAME") })
    allOutcomeTargetNames <- reactive({ queryDb("SELECT DISTINCT COHORT_NAME FROM @schema.TARGET ORDER BY COHORT_NAME") })

    output$outcomeCohorts <- renderUI(
    {
        df <- allOutcomeCohortNames()
        picker <- pickerInput(
          "outcomeCohorts",
          "Disease outcomes:",
          choices = df$COHORT_NAME,
          selected = df$COHORT_NAME,
          options = shinyWidgets::pickerOptions(actionsBox = TRUE, liveSearch = TRUE),
          multiple = TRUE
        )
        return(picker)
    })

    output$targetCohorts <- renderUI(
    {
        df <- allOutcomeTargetNames()
        picker <- pickerInput(
          "targetCohorts",
          "Drug exposures:",
          choices = df$COHORT_NAME,
          selected = df$COHORT_NAME,
          options = shinyWidgets::pickerOptions(actionsBox = TRUE, liveSearch = TRUE),
          multiple = TRUE
        )
        return(picker)
    })

    # Subset of results for harm, risk and treatement categories
    mainTableRiskHarmFilters <- reactive({
        df <- mainTableRe()
        filtered <- df[df$OUTCOME_COHORT_NAME %in% input$outcomeCohorts
                         & df$TARGET_COHORT_NAME %in% input$targetCohorts, ]
        return(filtered)
    })

    output$mainTable <- DT::renderDataTable({
        df <- mainTableRiskHarmFilters()
        tryCatch(
            {
            df$I2 <- formatC(df$I2, digits = 2, format = "f")
            colnames(df)[colnames(df) == "I2"] <- "I-squared"
            colnames(df)[colnames(df) == "RISK_COUNT"] <- "Sources with scc risk"
            colnames(df)[colnames(df) == "BENEFIT_COUNT"] <- "Sources with scc benefit"
            colnames(df)[colnames(df) == "OUTCOME_COHORT_NAME"] <- "Outcome cohort name"
            colnames(df)[colnames(df) == "TARGET_COHORT_NAME"] <- "Exposure"
            colnames(df)[colnames(df) == "TARGET_COHORT_ID"] <- "Target cohort id"
            colnames(df)[colnames(df) == "OUTCOME_COHORT_ID"] <- "Outcome cohort id"

            table <- DT::datatable(
              df, selection = "single",
              rownames = FALSE
            )
            return(table)
        },
        # Handles messy response
        error = function(e) {
            return(DT::datatable(data.frame()))
        }
        )
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

    dynamicMetaAnalysisTbl <- reactive({
        s <- filteredTableSelected()
        treatment <- s$TARGET_COHORT_ID
        outcome <- s$OUTCOME_COHORT_ID
        if (length(outcome)) {
            updateTabsetPanel(session, "mainPanel", "Detail")
            sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRows.sql", package = "rewardb"))
            uncalibratedTable <- queryDb(sql, treatment = treatment, outcome = outcome, calibrated=0)
            calibratedTable <- queryDb(sql, treatment = treatment, outcome = outcome, calibrated=1)

            if (nrow(calibratedTable)) {
                calibratedTable$CALIBRATED = 1
                uncalibratedTable$CALIBRATED = 0
                uncalibratedTable$SOURCE_NAME <- paste(uncalibratedTable$SOURCE_NAME, "Uncalibrated")
                calibratedTable$SOURCE_NAME <- paste(calibratedTable$SOURCE_NAME, "Calibrated")
                table <- rbind(calibratedTable, uncalibratedTable)
                return(table[order(table$SOURCE_ID, table$SOURCE_NAME),])
            }
            return(uncalibratedTable)
        }
        return(data.frame())
    })

    fullResultsTable <- function() {
        option = list(columnDefs = list(list(targets = c(8, 11), class = "dt-right")))
        table3 <- dynamicMetaAnalysisTbl()
        if(nrow(table3) >= 1) {
            table3$RR[table3$RR > 100] <- NA
            table3$C_PT <- formatC(table3$C_PT, digits = 0, format = "f")
            table3$T_PT <- formatC(table3$T_PT, digits = 0, format = "f")
            table3$RR <- formatC(table3$RR, digits = 2, format = "f")
            table3$LB_95 <- formatC(table3$LB_95, digits = 2, format = "f")
            table3$UB_95 <- formatC(table3$UB_95, digits = 2, format = "f")
            table3$P_VALUE <- formatC(table3$P_VALUE, digits = 2, format = "f")
            table3$I2 <- formatC(table3$I2, digits = 2, format = "f")

            for (n in names(niceColumnName)) {
                colnames(table3)[colnames(table3) == n] <- niceColumnName[n]
            }

            headers <- names(niceColumnNameInv)
            table4 <- DT::datatable(table3[, headers], rownames = FALSE, escape = FALSE, )
            return(table4)
        }
    }

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

    output$forestPlot <- renderPlot({
        df <- dynamicMetaAnalysisTbl()
        if (nrow(df)) {
            return(rewardb::forestPlot(df))
        }
    })
}