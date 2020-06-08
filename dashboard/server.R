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
        log_event(paste("filtering table - sql query"))
        mainTableSql <- readr::read_file("sql/mainTable.sql")
        risk <- input$cutrange2
        benefit <- input$cutrange1
        df <- DatabaseConnector::renderTranslateQuerySql(dbConn, mainTableSql, riskThreshold = risk, benefitThreshold = benefit)
        return(df)
    })
    
    # Subset of results for harm, risk and treatement categories
    mainTableRiskHarmFilters <- reactive({
        df <- mainTableRe()
        log_event("filtering table - ui")
        filtered <- df[df$OUTCOME_COHORT_NAME %in% input$outcomeCohorts & df$TARGET_COHORT_NAME %in% input$targetCohorts & 
            df$SC_RISK %in% input$scRisk & df$SC_BENEFIT %in% input$scBenefit, ]
        return(filtered)
    })
    
    output$mainTable <- DT::renderDataTable({
        df <- mainTableRiskHarmFilters()
        cleaned <- df[, !names(df) %in% c("OUTCOME_COHORT_ID", "TARGET_COHORT_ID")]
        DT::datatable(cleaned, selection = "single", rownames = FALSE, escape = FALSE)
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
    
    output$selectTreatement <- renderUI({
        df <- mainTableRe()
        widget <- shinyWidgets::pickerInput("targetCohorts", "Drug Exposures:", choices = unique(df$TARGET_COHORT_NAME), 
            selected = unique(df$TARGET_COHORT_NAME), options = shinyWidgets::pickerOptions(actionsBox = TRUE, liveSearch = TRUE), 
            multiple = TRUE)
    })
    
    output$selectOutcome <- renderUI({
        df <- mainTableRe()
        shinyWidgets::pickerInput("outcomeCohorts", "Outcomes:", choices = unique(df$OUTCOME_COHORT_NAME), selected = unique(df$OUTCOME_COHORT_NAME), 
            options = shinyWidgets::pickerOptions(actionsBox = TRUE, liveSearch = TRUE), multiple = TRUE)
    })
    
    dynamicMetaAnalysisTbl <- reactive({
        s <- filteredTableSelected()
        if (!is.null(s$TARGET_COHORT_ID)) {
            updateTabsetPanel(session, "mainPanel", "Detail")
            treatment <- s$TARGET_COHORT_ID
            outcome <- s$OUTCOME_COHORT_ID
            return(getMetaAnalysisData(dbConn, treatment, outcome))
        }
        NULL
    })
    
    fullResultsTable <- function() {
        option = list(columnDefs = list(list(targets = c(8, 11), class = "dt-right")))
        table3 <- dynamicMetaAnalysisTbl()
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
    
    output$fullResultsTable <- DT::renderDataTable({
        fullResultsTable()
    })
    
    output$forestPlot <- renderPlot({
        df <- dynamicMetaAnalysisTbl()
        if (!is.null(df)) {
            return(forestPlot(df))
        }
    })
    
    output$eOutcomeProb <- renderPlotly({
        selectedInput <- filteredTableSelected()
        target <- selectedInput$TARGET_COHORT_ID
        outcome <- selectedInput$OUTCOME_COHORT_ID
        sql <- "SELECT * FROM results WHERE TARGET_COHORT_ID = @target AND OUTCOME_COHORT_ID = @outcome ORDER BY SOURCE_ID"
        dfScores <- DatabaseConnector::renderTranslateQuerySql(dbConn, sql, target = target, outcome = outcome)
        sql <- "SELECT * FROM results WHERE TARGET_COHORT_ID = @target"
        df <- DatabaseConnector::renderTranslateQuerySql(dbConn, sql, target = target)
        plot <- outcomeDistribution(df, dfScores, target, outcome)
    })
    
    manhattanRes <- eventReactive(input$querySql, {
        shinyEventLogger::log_output(paste("filtering m plot - sql query"))
        manhattanSql <- readr::read_file("sql/plots.sql")
        df <- DatabaseConnector::renderTranslateQuerySql(dbConn, manhattanSql, benefitThreshold = input$cutrange[1], 
            harmThreshold = input$cutrange[2])
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
