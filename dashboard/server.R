library(shiny)
library(shinyWidgets)
library(scales)
library(DT)
library(rewardb)

server <- function(input, output, session) {
    # Query full results, only filter is Risk range parameters
    mainTableRe <- reactive({
        benefit <- input$cutrange1
        risk <- input$cutrange2
        mainTableSql <- readr::read_file(system.file("sql/queries/", "mainTable.sql", package = "rewardb"))
        calibrated <- ifelse(input$calibrated, 1, 0)
        bSelection <- paste0("'", paste0(input$scBenefit, sep="'"))
        rSelection <- paste0("'", paste0(input$scRisk, sep="'"))
        st <- Sys.time()
        df <- queryDb(mainTableSql, risk = risk, benefit = benefit,
                      risk_selection = rSelection, benefit_selection = bSelection, calibrated=calibrated)
        return(df)
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
        table <- DT::datatable(
          df, selection = "single",
          rownames = FALSE
        )
        return(table)
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
            table4 <- DT::datatable(table3[, headers], rownames = FALSE, escape = FALSE)
            return(table4)
        }
    }

    resultsDownload <- function () {

    }

    output$fullResultsTable <- DT::renderDataTable({
        fullResultsTable()
    })

    output$forestPlot <- renderPlot({
        df <- dynamicMetaAnalysisTbl()
        if (nrow(df)) {
            return(rewardb::forestPlot(df))
        }
    })
}
