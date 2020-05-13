library(shiny)
library(shinyWidgets)
library(ggplot2)
library(scales)
library(DT)
library(plotly)

source("plots.R")
### SERVER SCRIPT ###
server <- function(input, output, session) {
  log_event("server")
  # Query full results, only filter is Risk range parameters
  mainTableRe <- eventReactive(input$querySql, {
    log_event(paste("filtering table - sql query"))
    mainTableSql <- readr::read_file("sql/mainTable.sql")
    df <- DatabaseConnector::renderTranslateQuerySql(dbConn, mainTableSql,
                                                     benefitThreshold = input$cutrange[1],
                                                     harmThreshold = input$cutrange[2])
    return(df)
  })

  # Subset of results for harm, risk and treatement categories
  mainTableRiskHarmFilters <- reactive({
                                         df <- mainTableRe()
                                         log_event("filtering table - ui")
                                         filtered <- df[
                                           df$OUTCOME_COHORT_NAME %in% input$outcomeCohorts &
                                             df$TARGET_COHORT_NAME %in% input$targetCohorts &
                                             df$EXPOSURE_CLASS %in% input$exposureClasses &
                                             df$SC_RISK %in% input$scRisk &
                                             df$SC_BENEFIT %in% input$scBenefit,]
                                         return(filtered)
                                       })

  output$mainTable <- DT::renderDataTable({
                                            df <- mainTableRiskHarmFilters()
                                            cleaned <- df[, !names(df) %in% c("OUTCOME_COHORT_ID", "TARGET_COHORT_ID")]
                                            DT::datatable(cleaned, selection = 'single', rownames = FALSE, escape = FALSE)
                                          })

  filteredTableSelected <- reactive({
                                      ids <- input$mainTable_rows_selected
                                      filtered1 <- mainTableRiskHarmFilters()
                                      filtered2 <- filtered1[ids,]
                                      return(filtered2)
                                    })

  output$treatmentOutcomeStr <- renderText({
                                             s <- filteredTableSelected()
                                             return(paste(s$TARGET_COHORT_NAME, s$TARGET_COHORT_ID, "for", s$OUTCOME_COHORT_NAME, s$OUTCOME_COHORT_ID))
                                           })

  output$selectTreatement <- renderUI({
                                        df <- mainTableRe()
                                        widget <- shinyWidgets::pickerInput("targetCohorts", "Drug Exposures:",
                                                                            choices = unique(df$TARGET_COHORT_NAME),
                                                                            selected = unique(df$TARGET_COHORT_NAME),
                                                                            options = shinyWidgets::pickerOptions(actionsBox = TRUE, liveSearch = TRUE),
                                                                            multiple = TRUE)
                                      })

  output$selectOutcome <- renderUI({
                                     df <- mainTableRe()
                                     shinyWidgets::pickerInput("outcomeCohorts", "Outcomes:",
                                                               choices = unique(df$OUTCOME_COHORT_NAME),
                                                               selected = unique(df$OUTCOME_COHORT_NAME),
                                                               options = shinyWidgets::pickerOptions(actionsBox = TRUE, liveSearch = TRUE),
                                                               multiple = TRUE)
                                   })

  fullResultsTable <- function() {
    option = list(columnDefs = list(list(targets = c(8, 11), class = "dt-right")))
    s <- filteredTableSelected()
    updateTabsetPanel(session, "mainPanel", "Detail")
    treatment <- s$TARGET_COHORT_ID
    outcome <- s$OUTCOME_COHORT_ID

    log_event(s)
    sql <- "SELECT * FROM full_results WHERE OUTCOME_COHORT_ID = @outcome AND TARGET_COHORT_ID = @treatment ORDER BY SOURCE_ID"
    table3 <- DatabaseConnector::renderTranslateQuerySql(dbConn, sql, treatment = treatment, outcome = outcome)
    table3$RR[table3$RR > 100] <- NA
    table3$C_PT <- format(table3$C_PT, digits = 0, format = "f")
    table3$T_PT <- format(table3$T_PT, digits = 0, format = "f")
    table3$RR <- formatC(table3$RR, digits = 2, format = "f")
    table3$LB_95 <- formatC(table3$LB_95, digits = 2, format = "f")
    table3$UB_95 <- formatC(table3$UB_95, digits = 2, format = "f")
    table3$P_VALUE <- formatC(table3$P_VALUE, digits = 2, format = "f")
    table3$I2 <- formatC(table3$I2, digits = 2, format = "f")
    table3$C_PT <- formatC(table3$C_PT, digits = 0, format = "f")
    table3$T_PT <- formatC(table3$T_PT, digits = 0, format = "f")


    for (n in names(niceColumnName)) {
      colnames(table3)[colnames(table3) == n] <- niceColumnName[n]
    }

    headers <- names(niceColumnNameInv)
    table4 <- DT::datatable(table3[, headers], rownames = FALSE, escape = FALSE)
    return(table4)
  }

  output$fullResultsTable <- DT::renderDataTable({ fullResultsTable() })

  output$forestPlot <- renderPlot({
                                    selectedInput <- filteredTableSelected()
                                    treatment <- selectedInput$TARGET_COHORT_ID
                                    outcome <- selectedInput$OUTCOME_COHORT_ID
                                    sql <- "SELECT * FROM full_results WHERE TARGET_COHORT_ID = @target AND OUTCOME_COHORT_ID = @outcome ORDER BY SOURCE_ID";
                                    df <- DatabaseConnector::renderTranslateQuerySql(dbConn, sql, target = treatment, outcome = outcome)
                                    forestPlot(df)
                                  })

  manhattanRes <- eventReactive(input$querySql, {
    shinyEventLogger::log_output(paste("filtering m plot - sql query"))
    manhattanSql <- readr::read_file("sql/plots.sql")
    df <- DatabaseConnector::renderTranslateQuerySql(dbConn, manhattanSql,
                                                     benefitThreshold = input$cutrange[1],
                                                     harmThreshold = input$cutrange[2])
    return(df)
  })

  dataSrc <- function() {
    df <- manhattanRes()
    ds <- unique(df$SOURCE_NAME)
    picker <- shinyWidgets::pickerInput("selectDataSource", "Data Sources",
                                        choices = ds,
                                        selected = ds,
                                        options = shinyWidgets::pickerOptions(actionsBox = TRUE),
                                        multiple = TRUE)
    return(picker)
  }
  output$selectDataSource <- renderUI({ dataSrc() })

  dataSources <- reactive({ input$selectDataSource })
  manhattanFilter <- function() {
    df <- manhattanRes()
    log_event(paste("filtering m plot - ui query"))
    filtered <- df[
      df$SOURCE_NAME %in% dataSources() &
        df$OUTCOME_COHORT_NAME %in% input$outcomeCohorts &
        df$TARGET_COHORT_NAME %in% input$targetCohorts &
        df$EXPOSURE_CLASS %in% input$exposureClasses &
        df$SC_RISK %in% input$scRisk &
        df$SC_BENEFIT %in% input$scBenefit,]
    return(filtered)
  }
  manhattanResFiltered <- reactive({ manhattanFilter() })

  xCol <- reactive({ input$manXColumn })

  uiElem <- function() {
    df <- manhattanResFiltered()
    picker <- shinyWidgets::pickerInput("manXColumn", "x axis", choices = names(df), selected = "TARGET_COHORT_NAME")
    return(picker)
  }
  output$selectMx <- renderUI({ uiElem() })

  mplotSelect <- reactive({ input$mplotType })
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