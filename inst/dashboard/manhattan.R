manhattanPlotServer <- function(input, output, session) {
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
##### UI ######
manhattanPlotPanel <- tabPanel(
  "Plots",
  HTML("<h4> Plot configuration </h4>"),
  fluidRow(
    column(2,
           pickerInput("mplotType", "Plot type",
                       choices = c("Manhattan", "Distribution")
           )
    ),
    column(2,
           pickerInput("yFunc", "Y Function",
                       choices = c("RR", "log(RR)", "1/RR", "-1 * RR", "-1 * log(RR)", "P_VALUE", "LB_95", "UB_95")
           )
    ),
    column(2, uiOutput("selectMx")),
    column(2, uiOutput("selectDataSource")),
  ),
  plotly::plotlyOutput("mplot"),
  div(strong("Figure 3."), textOutput("mplotFigureTitle")
  )
)

irrTab <- tabPanel(
  "IRR probability",
  plotly::plotlyOutput("eOutcomeProb", height = 800),
  div(
    strong("Figure 2."),
    paste("Kernel Density Estimates of IRR scores for all outcomes in data set for ", textOutput("targetStr"))
  )
)