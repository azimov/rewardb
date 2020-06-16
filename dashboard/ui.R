library(shiny)
library(shinyWidgets)

filterSql <- "SELECT DISTINCT(OUTCOME_COHORT_ID), COHORT_NAME AS OUTCOME_COHORT_NAME FROM @schema.outcome"
outcomes <- queryDb(filterSql)

filterSql <- "SELECT DISTINCT(TARGET_COHORT_ID), COHORT_NAME AS TARGET_COHORT_NAME FROM @schema.target"
treatments <- queryDb(filterSql)

exposureClassesSql <- "SELECT DISTINCT(EXPOSURE_CLASS) FROM TREATMENT_CLASSES ORDER BY EXPOSURE_CLASS"
exposureClasses <- c()  # DatabaseConnector::renderTranslateQuerySql(dbConn, exposureClassesSql)

metaDisplayCondtion <- "typeof input.mainTable_rows_selected  !== 'undefined' && input.mainTable_rows_selected.length > 0"
metaResultsPanel <- conditionalPanel(
  condition = metaDisplayCondtion,
  HTML(paste("<h4 id='mainR'>", textOutput("treatmentOutcomeStr"), "</h4>")),
  tabsetPanel(
    id = "tabsetPanelResults",
    tabPanel("Detailed results", DT::dataTableOutput("fullResultsTable")),
    tabPanel(
      "Forest plot",
      plotOutput("forestPlot", height = 800, hover = hoverOpts("plotHoverForestPlot")),
      div(strong("Figure 1."), "Forest plot of effect estimates from each database")
    )
  )
)
mainPanelOutput <- tabPanel("Main Results", DT::dataTableOutput("mainTable"), metaResultsPanel)

sidePane <- fluidRow(
  column(
    2,
    sliderInput("cutrange1", "Benefit Threshold:", min = 0.1, max = 0.9, step = 0.1, value = 0.5),
    sliderInput("cutrange2", "Risk Threshold:", min = 1.1, max = 2.5, step = 0.1, value = 2),
    checkboxInput("calibrated", "Use calibrated results", FALSE),
    pickerInput(
      "targetCohorts",
      "Drug Exposures:",
      choices = treatments$TARGET_COHORT_NAME,
      selected = treatments$TARGET_COHORT_NAME,
      options = shinyWidgets::pickerOptions(actionsBox = TRUE, liveSearch = TRUE),
      multiple = TRUE
    ),
    pickerInput(
      "outcomeCohorts",
      "Outcomes:",
      choices = outcomes$OUTCOME_COHORT_NAME,
      selected = outcomes$OUTCOME_COHORT_NAME,
      options = shinyWidgets::pickerOptions(actionsBox = TRUE, liveSearch = TRUE),
      multiple = TRUE
    ),
    pickerInput(
      "exposureClasses",
      "Exposure Classes:",
      choices = exposureClasses$EXPOSURE_CLASS,
      selected = exposureClasses$EXPOSURE_CLASS,
      options = shinyWidgets::pickerOptions(actionsBox = TRUE, liveSearch = TRUE),
      multiple = TRUE
    ),
    pickerInput(
      "scBenefit",
      "Sources with self control benefit:",
      choices = scBenefitRisk,
      selected = "all",
      options = shinyWidgets::pickerOptions(actionsBox = TRUE),
      multiple = TRUE
    ),
    pickerInput(
      "scRisk",
      "Sources with self control risk:",
      choices = scBenefitRisk,
      selected = "none",
      options = shinyWidgets::pickerOptions(actionsBox = TRUE),
      multiple = TRUE
    ),
    downloadButton(
      "downloadtable",
      "Download file",
      style = "display: block; margin: 0 auto; color: blue;"
    )
  ),
  column(9, tabsetPanel(id = "mainPanel", mainPanelOutput))
)


### UI Script ###
ui <- fluidPage(
  titlePanel(
    HTML(paste("<h1>REWARD-B Dashboard", appContext$name, "</h1>")),
    windowTitle = paste("REWARD-B Dashboard - ", appContext$name)
  ),
  sidePane
)
