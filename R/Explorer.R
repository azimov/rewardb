#' @title
#' reportInstance
#' @description
#' Requires a server appContext instance to be loaded in environment see scoping of launchDashboard
#' This can be obtained with rewardb::loadAppContext(...)
#' UNDER DEVELOPMENT
#' @param input shiny input object
#' @param output shiny output object
#' @param session shiny session object
#' @importFrom gt gt fmt_number render_gt tab_options
#' @import shiny
explorerServer <- function(input, output, session) {
  secureApplication <- getOption("reward.secure", default = FALSE)

  if (secureApplication) {
    # define some credentials
    credentials <- data.frame(
      user = c("reward_user"), # mandatory
      password = c(model$config$connectionDetails$password()), # mandatory
      admin = c(FALSE)
    )

    res_auth <- secure_server(
      check_credentials = shinymanager::check_credentials(credentials)
    )

    output$auth_output <- shiny::renderPrint({
      shiny::reactiveValuesToList(res_auth)
    })
  }

  message("Init report dashboard")

  exposureCohorts <- model$getExposureCohorts()
  outcomeCohorts <- model$getOutcomeCohorts()
  dataSources <- model$getDataSourceInfo()

  shiny::updateSelectizeInput(session, "targetCohorts", choices = exposureCohorts$cohortDefinitionName, server = TRUE)
  shiny::updateSelectizeInput(session, "outcomeCohorts", choices = outcomeCohorts$cohortDefinitionName, server = TRUE)
  shinyWidgets::updatePickerInput(session, "dataSourcesUsed", choices = dataSources$sourceName, selected = dataSources$sourceName)
  selectedOutcomeTypes <- shiny::reactive({
    if (is.null(input$selectedOutcomeTypes)) {
      return(0:3)
    }
    as.integer(input$selectedOutcomeTypes)
  })

  filteredOutcomeCohorts <- shiny::reactive({
    selected <- selectedOutcomeTypes()
    outcomeCohorts %>%
      dplyr::filter(outcomeType %in% selected)
  })

  shiny::observe({
    input$selectedOutcomeTypes
    cohorts <- filteredOutcomeCohorts()
    shiny::updateSelectizeInput(session, "outcomeCohorts", choices = cohorts$cohortDefinitionName, server = TRUE)
  })

  output$outcomeCohortsTable <- DT::renderDataTable({
    oc <- filteredOutcomeCohorts()
    oc %>% dplyr::mutate(outcomeType = dplyr::recode(outcomeType,
                                                     "0" = "TWO DX",
                                                     "2" = "ONE DX",
                                                     "1" = "Inpatient",
                                                     "3" = "ATLAS"))
  })

  selectedExposureTypes <- shiny::reactive({
    if (is.null(input$selectedExposureTypes)) {
      return(-1:1)
    }
    as.integer(input$selectedExposureTypes)
  })

  filteredExposureCohorts <- shiny::reactive({
    selected <- selectedExposureTypes()
    exposureCohorts %>% dplyr::filter(atcFlg %in% selected)
  })

  shiny::observe({
    input$selectedExposureTypes
    cohorts <- filteredExposureCohorts()
    shiny::updateSelectizeInput(session, "targetCohorts", choices = cohorts$cohortDefinitionName, server = TRUE)
  })


  output$exposureCohortsTable <- DT::renderDataTable({
    filteredExposureCohorts()
  })

  output$dataSourcesTable <- DT::renderDataTable({
    model$getDataSourceInfo()
  })

  output$dataQaulityTable <- gt::render_gt({
    exposureOutcomePairs <- data.frame(
      exposureId = c(7869, 7869, 8163, 8163),
      outcomeId = c(311525, 311526, 345050, 345074)
    )

    baseData <- model$getExposureOutcomeDqd(exposureOutcomePairs) %>%
      gt::gt() %>%
      gt::fmt_number(decimals = 2, columns = c(3, 4, 7, 8)) %>%
      gt::tab_options(
        table.font.size = "tiny"
      )

    return(baseData)
  })

  getExposureCohort <- shiny::reactive({
    exposureCohorts %>% dplyr::filter(cohortDefinitionName %in% input$targetCohorts)
  })

  getOutcomeCohort <- shiny::reactive({
    outcomeCohorts %>% dplyr::filter(cohortDefinitionName %in% input$outcomeCohorts)
  })

  output$selectedCohorts <- shiny::renderText("not selected")

  shiny::observeEvent(input$selectCohorts, {
    output$selectedCohorts <- shiny::renderText("selected")
  })

  selectedExposureOutcome <- shiny::eventReactive(input$selectCohorts, {
    exposureCohort <- getExposureCohort()
    outcomeCohort <- getOutcomeCohort()
    if (is.null(exposureCohort) || is.null(outcomeCohort)) {
      return(NULL)
    }
    selected <- list(
      TARGET_COHORT_ID = exposureCohort$cohortDefinitionId,
      TARGET_COHORT_NAME = exposureCohort$cohortDefinitionName,
      OUTCOME_COHORT_ID = outcomeCohort$cohortDefinitionId,
      OUTCOME_COHORT_NAME = outcomeCohort$cohortDefinitionName,
      calibrationType = input$calibrationType,
      usedDataSources = c(dataSources[dataSources$sourceName %in% input$dataSourcesUsed,]$sourceId, -99)
    )
    return(selected)
  })

  output$selectedExposureId <- shiny::renderText({
    s <- selectedExposureOutcome()
    if (is.null(s)) {
      return("")
    }
    return(s$TARGET_COHORT_ID)
  })

  output$selectedOutcomeId <- shiny::renderText({
    s <- selectedExposureOutcome()
    if (is.null(s)) {
      return("")
    }
    return(s$OUTCOME_COHORT_ID)
  })

  output$treatmentOutcomeStr <- shiny::renderText({
    s <- selectedExposureOutcome()
    ParallelLogger::logDebug("selected")
    if (is.null(s)) {
      return("No cohorts selected")
    }
    return(paste("Exposure of", s$TARGET_COHORT_NAME, "for outcome of", s$OUTCOME_COHORT_NAME))
  })

  ParallelLogger::logDebug("init modules")
  # Create sub modules
  metaAnalysisTableServer("metaTable", model, selectedExposureOutcome)
  forestPlotServer("forestPlot", model, selectedExposureOutcome)

  calibrationPlotServer("outcomeCalibrationPlot", model, selectedExposureOutcome, FALSE)
  calibrationPlotServer("exposureCalibrationPlot", model, selectedExposureOutcome, TRUE)

  timeOnTreatmentServer("timeOnTreatment", model, selectedExposureOutcome)
  tabPanelTimeOnTreatment <- shiny::tabPanel("Time on treatment", boxPlotModuleUi("timeOnTreatment"))
  shiny::appendTab(inputId = "searchResults", tabPanelTimeOnTreatment)

  timeToOutcomeServer("timeToOutcome", model, selectedExposureOutcome)
  tabPanelTimeToOutcome <- shiny::tabPanel("Time to outcome", boxPlotModuleUi("timeToOutcome"))
  shiny::appendTab(inputId = "searchResults", tabPanelTimeToOutcome)

  ingredientConceptInput <- shiny::reactive({
    selected <- selectedExposureOutcome()
    if (is.null(selected))
      return(data.frame())
    model$getExposureConceptSet(selected$TARGET_COHORT_ID)
  })

  conditionConceptInput <- shiny::reactive({
    selected <- selectedExposureOutcome()
    if (is.null(selected))
      return(data.frame())
    model$getOutcomeConceptSet(selected$OUTCOME_COHORT_ID)
  })

  output$selectedOutcomeConceptSet <- DT::renderDataTable({ conditionConceptInput() })
  output$selectedExposureConceptSet <- DT::renderDataTable({ ingredientConceptInput() })

  # Add cem panel if option is present
  if (!is.null(reportAppContext$cemConnectionDetails)) {
    message("loading cem api")
    cemBackend <- do.call(CemConnector::createCemConnection, reportAppContext$cemConnectionDetails)
    ceModuleServer <- CemConnector::ceExplorerModule("cemExplorer",
                                                     cemBackend,
                                                     ingredientConceptInput = ingredientConceptInput,
                                                     conditionConceptInput = conditionConceptInput,
                                                     siblingLookupLevelsInput = shiny::reactive({ 0 }))
    cemPanel <- shiny::tabPanel("Evidence", CemConnector::ceExplorerModuleUi("cemExplorer"))
    shiny::appendTab(inputId = "searchResults", cemPanel)
  }
}

#' @import shiny
#' @import shinydashboard
explorerUi <- function(request) {
  resultsDisplayCondition <- "output.selectedCohorts == 'selected'"
  riskEstimatesPanel <- shiny::tabsetPanel(id = "searchResults",
                                           shiny::tabPanel("Risk Estimates",
                                                           shiny::tags$h4("Datasource Results and Meta-analysis"),
                                                           metaAnalysisTableUi("metaTable"),
                                                           shiny::tags$h4("Forest plot"),
                                                           forestPlotUi("forestPlot")),
                                           shiny::tabPanel("Outcome Controls",
                                                           calibrationPlotUi("outcomeCalibrationPlot")),
                                           shiny::tabPanel("Exposure Controls",
                                                           calibrationPlotUi("exposureCalibrationPlot")))

  searchPanel <- shinydashboard::tabItem("Search",
                                         shiny::fluidRow(
                                           shinydashboard::box(width = 12,
                                                               shiny::column(6,
                                                                             shiny::selectizeInput("targetCohorts",
                                                                                                   label = "Drug exposures:",
                                                                                                   choices = NULL,
                                                                                                   multiple = FALSE,
                                                                                                   width = 500)),
                                                               shiny::column(6,
                                                                             shiny::selectizeInput("outcomeCohorts",
                                                                                                   label = "Disease outcomes:",
                                                                                                   choices = NULL,
                                                                                                   multiple = FALSE,
                                                                                                   width = 500)),
                                                               shiny::column(6,
                                                                             shiny::selectizeInput("calibrationType",
                                                                                                   label = "Calibration Controls:",
                                                                                                   choices = c("exposures", "outcomes"),
                                                                                                   selected = "outcomes",
                                                                                                   multiple = FALSE, width = 500)),
                                                               shiny::column(6,
                                                                             shinyWidgets::pickerInput("dataSourcesUsed",
                                                                                                       label = "Data Sources:",
                                                                                                       choices = NULL,
                                                                                                       multiple = TRUE, width = 500)),
                                                               shiny::column(12,

                                                                             shiny::actionButton("selectCohorts", "Select"))),
                                           shinydashboard::box(width = 12,
                                                               shiny::conditionalPanel(condition = resultsDisplayCondition,
                                                                                       shiny::tags$h3(textOutput("treatmentOutcomeStr")), riskEstimatesPanel),

                                                               shiny::conditionalPanel(condition = "output.selectedCohorts != 'selected'",
                                                                                       shiny::textOutput("selectedCohorts")))))

  aboutTab <- shinydashboard::tabItem("About",
                                      shiny::fluidRow(shinydashboard::box(width = 12,
                                                                          shiny::h1("REWARD - all by all explorer"),
                                                                          shiny::includeHTML(system.file("static_html", "about_rewardb.html", package = "rewardb")),
                                                                          shinydashboard::box(width = 12,
                                                                                              shiny::tags$h3("Registered data sources"),
                                                                                              shinycssloaders::withSpinner(DT::dataTableOutput("dataSourcesTable"))),
                                                                          shinydashboard::box(width = 12,
                                                                                              shiny::tags$h3("Data quality Indicator Pairs"),
                                                                                              shinycssloaders::withSpinner(gt::gt_output(outputId = "dataQaulityTable"))))))
  exposureCohortsTab <- shinydashboard::tabItem("exposureCohortsTab",
                                                shinydashboard::box(width = 12,
                                                                    shinycssloaders::withSpinner(DT::dataTableOutput("exposureCohortsTable"))))

  outcomeCohortsTab <- shinydashboard::tabItem("outcomeCohortsTab",
                                               shiny::fluidRow(
                                                 shinydashboard::box(width = 12,
                                                                     shinycssloaders::withSpinner(DT::dataTableOutput("outcomeCohortsTable")))))

  body <- shinydashboard::dashboardBody(shinydashboard::tabItems(aboutTab,
                                                                 searchPanel,
                                                                 exposureCohortsTab,
                                                                 outcomeCohortsTab))

  outcomeCohortSelection <- shinyWidgets::pickerInput("selectedOutcomeTypes",
                                                      label = "Outcome Cohort Types:",
                                                      choices = c(
                                                        "2 Diagnosis Codes" = 0,
                                                        "Inpatient Visits" = 1,
                                                        "1 Diagnosis code" = 2,
                                                        "ATLAS" = 3
                                                      ),
                                                      selected = c(),
                                                      options = shinyWidgets::pickerOptions(
                                                        actionsBox = TRUE,
                                                        noneSelectedText = "Filter by subset"
                                                      ),
                                                      multiple = TRUE)

  exposureCohortSelection <- shinyWidgets::pickerInput("selectedExposureTypes",
                                                       label = "Exposure Cohort Types:",
                                                       choices = c(
                                                         "RxNorm Ingredient" = 0,
                                                         "ATC 3" = 1,
                                                         "ATLAS" = -1
                                                       ),
                                                       selected = c(),
                                                       options = shinyWidgets::pickerOptions(
                                                         actionsBox = TRUE,
                                                         noneSelectedText = "Filter by subset"
                                                       ),
                                                       multiple = TRUE)

  sidebar <- shinydashboard::dashboardSidebar(shinydashboard::sidebarMenu(shinydashboard::menuItem("About", tabName = "About", icon = shiny::icon("rectangle-list")),
                                                                          shinydashboard::menuItem("Search", tabName = "Search", icon = shiny::icon("table")),
                                                                          shinydashboard::menuItem("Available Exposures", tabName = "exposureCohortsTab", icon = shiny::icon("table")),
                                                                          shinydashboard::menuItem("Available outcomes", tabName = "outcomeCohortsTab", icon = shiny::icon("table")),
                                                                          outcomeCohortSelection,
                                                                          exposureCohortSelection))


  shinydashboard::dashboardPage(shinydashboard::dashboardHeader(title = "REWARD: All Exposures by all outcomes"),
                                sidebar,
                                body)

}


#' @title
#' Launch the REWARD Shiny app report
#' @description
#' Launches a Shiny app for a given configuration file
#' @param appConfigPath path to configuration file. This is loaded in to the local environment with the appContext variable
#' @import shiny
#' @export
launchExplorer <- function(globalConfigPath) {
  .GlobalEnv$reportAppContext <- loadReportContext(globalConfigPath)
  .GlobalEnv$model <- ReportDbModel(reportAppContext)

  secureApplication <- getOption("reward.secure", default = FALSE)

  if (secureApplication) {
    ui <- shinymanager::secure_app(explorerUi)
  } else {
    ui <- explorerUi
  }
  shiny::shinyApp(server = explorerServer, ui = ui, onStart = function() {
    shiny::onStop(function() {
      writeLines("Closing connection")
      model$closeConnection()
    })
  })
}