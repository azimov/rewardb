library(shiny)
library(shinyWidgets)
library(ggplot2)
library(scales)
library(DT)
library(plotly)
library(rewardb)

server <- function(input, output, session) {
    log_event("server")
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
        log_message(paste("main table - sql query took", round(Sys.time() - st, 3), "for rows:", nrow(df)))
        return(df)
    })

    # Subset of results for harm, risk and treatement categories
    mainTableRiskHarmFilters <- reactive({
        df <- mainTableRe()
        log_event("filtering table - ui")
        filtered <- df[df$OUTCOME_COHORT_NAME %in% input$outcomeCohorts
                         & df$TARGET_COHORT_NAME %in% input$targetCohorts, ]
        return(filtered)
    })

    output$mainTable <- DT::renderDataTable({
        st <- Sys.time()
        df <- mainTableRiskHarmFilters()
        log_message(paste("filtering - sql query + df filter took", round(Sys.time() - st, 3)))
        st <- Sys.time()
        table <- DT::datatable(
          df, selection = "single",
          rownames = FALSE
        )
        log_message(paste("making data table", round(Sys.time() - st, 3)))
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
            uncalibratedTable <- rewardb::getMetaAnalysisData(uncalibratedTable)

            calibratedTable <- queryDb(sql, treatment = treatment, outcome = outcome, calibrated=1)

            if (nrow(calibratedTable)) {
                calibratedTable <- rewardb::getMetaAnalysisData(calibratedTable)
                calibratedTable$CALIBRATED = 1
                uncalibratedTable$CALIBRATED = 0
                calibratedTable$SOURCE_NAME <- paste(calibratedTable$SOURCE_NAME, "Calibrated")
                uncalibratedTable$SOURCE_NAME <- paste(uncalibratedTable$SOURCE_NAME, "Uncalibrated")
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

    output$fullResultsTable <- DT::renderDataTable({
        fullResultsTable()
    })

    output$forestPlot <- renderPlot({
        df <- dynamicMetaAnalysisTbl()
        if (nrow(df)) {
            return(rewardb::forestPlot(df))
        }
    })

    output$eOutcomeProb <- renderPlotly({
        selectedInput <- filteredTableSelected()
        target <- selectedInput$TARGET_COHORT_ID
        outcome <- selectedInput$OUTCOME_COHORT_ID
        sql <- "SELECT * FROM results WHERE TARGET_COHORT_ID = @target AND OUTCOME_COHORT_ID = @outcome ORDER BY SOURCE_ID"
        dfScores <- queryDb( sql, target = target, outcome = outcome)
        sql <- "SELECT * FROM results WHERE TARGET_COHORT_ID = @target"
        df <- queryDb(sql, target = target)
        plot <- outcomeDistribution(df, dfScores, target, outcome)
    })

    manhattanRes <- eventReactive(input$querySql, {
        shinyEventLogger::log_output(paste("filtering m plot - sql query"))
        manhattanSql <- readr::read_file("sql/plots.sql")
        df <- queryDb(manhattanSql, benefitThreshold = input$cutrange1, harmThreshold = input$cutrange2)
        return(df)
    })

    dataSrc <- function() {
        df <- manhattanRes()
        ds <- unique(df$SOURCE_NAME)
        picker <- shinyWidgets::pickerInput("selectDataSource", "Data Sources", choices = ds, selected = ds, options = shinyWidgets::pickerOptions(actionsBox = TRUE),
            multiple = TRUE)
        return(picker)
    }
    output$selectDataSource <- renderUI({
        dataSrc()
    })

    dataSources <- reactive({
        input$selectDataSource
    })
    manhattanFilter <- function() {
        df <- manhattanRes()
        log_event(paste("filtering m plot - ui query"))
        filtered <- df[df$SOURCE_NAME %in% dataSources() & df$OUTCOME_COHORT_NAME %in% input$outcomeCohorts & df$TARGET_COHORT_NAME %in%
            input$targetCohorts & df$EXPOSURE_CLASS %in% input$exposureClasses & df$SC_RISK %in% input$scRisk & df$SC_BENEFIT %in%
            input$scBenefit, ]
        return(filtered)
    }
    manhattanResFiltered <- reactive({
        manhattanFilter()
    })

    xCol <- reactive({
        input$manXColumn
    })

    uiElem <- function() {
        df <- manhattanResFiltered()
        picker <- shinyWidgets::pickerInput("manXColumn", "x axis", choices = names(df), selected = "TARGET_COHORT_NAME")
        return(picker)
    }
    output$selectMx <- renderUI({
        uiElem()
    })

    mplotSelect <- reactive({
        input$mplotType
    })
    output$mplot <- renderPlotly({
        plotType <- mplotSelect()
        if (plotType == "Manhattan") {
            return(manhattanPlot(manhattanResFiltered, xCol, input$yFunc))
        } else if (plotType == "Distribution") {
            distPlot <- distPlot(manhattanResFiltered, xCol, input$yFunc)
            return(plotly::ggplotly(distPlot))
        }
    })
}
