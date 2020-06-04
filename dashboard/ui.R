library(shiny)
library(shinyWidgets)

filterSql <- "SELECT DISTINCT(OUTCOME_COHORT_ID), OUTCOME_COHORT_NAME FROM outcome"
outcomes <- DatabaseConnector::renderTranslateQuerySql(dbConn, filterSql)

filterSql <- "SELECT DISTINCT(TARGET_COHORT_ID), TARGET_COHORT_NAME FROM target"
treatments <- DatabaseConnector::renderTranslateQuerySql(dbConn, filterSql)

exposureClassesSql <- "SELECT DISTINCT(EXPOSURE_CLASS) FROM TREATMENT_CLASSES ORDER BY EXPOSURE_CLASS"
exposureClasses <- c()  # DatabaseConnector::renderTranslateQuerySql(dbConn, exposureClassesSql)

manhattanPlotPanel <- tabPanel("Plots", HTML("<h4> Plot configuration </h4>"), fluidRow(column(2, pickerInput("mplotType", 
    "Plot type", choices = c("Manhattan", "Distribution"))), column(2, pickerInput("yFunc", "Y Function", choices = c("RR", 
    "log(RR)", "1/RR", "-1 * RR", "-1 * log(RR)", "P_VALUE", "LB_95", "UB_95"))), column(2, uiOutput("selectMx")), 
    column(2, uiOutput("selectDataSource")), ), plotly::plotlyOutput("mplot"), div(strong("Figure 3."), textOutput("mplotFigureTitle")))

irrTab <- tabPanel("IRR probability", plotly::plotlyOutput("eOutcomeProb", height = 800), div(strong("Figure 2."), 
    paste("Kernel Density Estimates of IRR scores for all outcomes in data set for ", textOutput("targetStr"))))


mainPanelOutput <- tabPanel("Main Results", DT::dataTableOutput("mainTable"), conditionalPanel(condition = "typeof input.mainTable_rows_selected  !== 'undefined' && input.mainTable_rows_selected.length > 0", 
    HTML(paste("<h4 id='mainR'>", textOutput("treatmentOutcomeStr"), "</h4>")), tabsetPanel(id = "tabsetPanelResults", 
        tabPanel("Detailed results", DT::dataTableOutput("fullResultsTable")), tabPanel("Forest plot", plotOutput("forestPlot", 
            height = 500, hover = hoverOpts("plotHoverForestPlot")), div(strong("Figure 1."), "Forest plot of effect estimates from each database")))))

sidePane <- fluidRow(column(2, sliderInput("cutrange1", "Benefit Threshold:", min = 0.1, max = 0.9, step = 0.1, value = 0.5), 
    sliderInput("cutrange2", "Risk Threshold:", min = 1.1, max = 2.5, step = 0.1, value = 2), uiOutput("selectTreatement"), 
    uiOutput("selectOutcome"), pickerInput("exposureClasses", "Exposure Classes:", choices = exposureClasses$EXPOSURE_CLASS, 
        selected = exposureClasses$EXPOSURE_CLASS, options = shinyWidgets::pickerOptions(actionsBox = TRUE, liveSearch = TRUE), 
        multiple = TRUE), pickerInput("scBenefit", "Sources with self control benefit:", choices = scBenefitRisk, 
        selected = "all", options = shinyWidgets::pickerOptions(actionsBox = TRUE), multiple = TRUE), pickerInput("scRisk", 
        "Sources with self control risk:", choices = scBenefitRisk, selected = "none", options = shinyWidgets::pickerOptions(actionsBox = TRUE), 
        multiple = TRUE), downloadButton("downloadtable", "Download file", style = "display: block; margin: 0 auto; width: 230px;color: blue;")), 
    column(9, tabsetPanel(id = "mainPanel", mainPanelOutput)))


### UI Script ###
ui <- fluidPage(titlePanel(HTML(paste("<h1>REWARD-B Dashboard", appContext$name, "</h1>")), windowTitle = paste("REWARD-B Dashboard - ", 
    appContext$name)), sidePane)
