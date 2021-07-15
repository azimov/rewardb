#' @title
#' reportInstance
#' @description
#' Requires a server appContext instance to be loaded in environment see scoping of launchDashboard
#' This can be obtained with rewardb::loadAppContext(...)
#' UNDER DEVELOPMENT
#' @param input shiny input object
#' @param output shiny output object
#' @param session shiny session object
explorerServer <- function(input, output, session) {
  secureApplication <- getOption("reward.secure", default = FALSE)

  if (secureApplication) {
    # define some credentials
    credentials <- data.frame(
      user = c("reward_user", "jgilber2"), # mandatory
      password = c(model$config$connectionDetails$password(), "hangman252"), # mandatory
      admin = c(FALSE, TRUE)
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
  dataSources <-  model$getDataSourceInfo()

  message("Loaded startup cache")

  shiny::updateSelectizeInput(session, "outcomeCohorts", choices = outcomeCohorts$cohortDefinitionName, server = TRUE)
  shiny::updateSelectizeInput(session, "targetCohorts", choices = exposureCohorts$cohortDefinitionName, server = TRUE)
  shinyWidgets::updatePickerInput(session, "dataSourcesUsed", choices = dataSources$sourceName, selected = dataSources$sourceName)

  message("Loaded  inputs")

  output$outcomeCohortsTable <- DT::renderDataTable({
    outcomeCohorts
  })

  output$exposureCohortsTable <- DT::renderDataTable({
    exposureCohorts
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
    exposureCohorts[exposureCohorts$cohortDefinitionName %in% input$targetCohorts,]
  })

  getOutcomeCohort <- shiny::reactive({
    outcomeCohorts[outcomeCohorts$cohortDefinitionName %in% input$outcomeCohorts,]
  })

  output$selectedCohorts <- shiny::renderText("not selected")

  shiny::observeEvent(input$selectCohorts, {
    output$selectedCohorts <- shiny::renderText("selected")
  })

  selectedExposureOutcome <- shiny::reactive({
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
      usedDataSources = dataSources[dataSources$sourceName %in% input$dataSourcesUsed,]$sourceId
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

}

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
                                         shinydashboard::box(width = 12,
                                                             shiny::column(6,
                                                                           shiny::selectizeInput("targetCohorts", label = "Drug exposures:", choices = NULL, multiple = FALSE, width = 500)),
                                                             shiny::column(6,
                                                                           shiny::selectizeInput("outcomeCohorts", label = "Disease outcomes:", choices = NULL, multiple = FALSE, width = 500)),
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
                                                                           shiny::conditionalPanel(condition = "output.selectedCohorts != 'selected'",
                                                                                                   shiny::actionButton("selectCohorts", "Select")))),
                                         shinydashboard::box(width = 12,
                                                             shiny::conditionalPanel(condition = resultsDisplayCondition,
                                                                                     shiny::tags$h3(textOutput("treatmentOutcomeStr")), riskEstimatesPanel),

                                                             shiny::conditionalPanel(condition = "output.selectedCohorts != 'selected'",
                                                                                     shiny::textOutput("selectedCohorts"))))

  aboutTab <- shinydashboard::tabItem("About",
                                      shinydashboard::box(width = 12,
                                                          shiny::h1("REWARD - all by all explorer"),
                                                          shiny::p()))
  body <- shinydashboard::dashboardBody(shinydashboard::tabItems(aboutTab,
                                                 searchPanel,
                                                 shinydashboard::tabItem("sources",
                                                                         shinydashboard::box(width = 12,
                                                                                             shiny::tags$h3("Registered data sources"),
                                                                                             shinycssloaders::withSpinner(DT::dataTableOutput("dataSourcesTable"))),
                                                                         shinydashboard::box(width = 12,
                                                                                             shiny::tags$h3("Data quality Indicator Pairs"),
                                                                                             shinycssloaders::withSpinner(gt::gt_output(outputId = "dataQaulityTable")))),
                                                 shinydashboard::tabItem("exposureCohortsTab",
                                                                         shinydashboard::box(width = 12,
                                                                                             shinycssloaders::withSpinner(DT::dataTableOutput("exposureCohortsTable")))),
                                                 shinydashboard::tabItem("outcomeCohortsTab",
                                                                         shinydashboard::box(width = 12,
                                                                                             shinycssloaders::withSpinner(DT::dataTableOutput("outcomeCohortsTable"))))))

  sidebar <- shinydashboard::dashboardSidebar(shinydashboard::sidebarMenu(shinydashboard::menuItem("About", tabName = "About", icon = shiny::icon("list-alt")),
                                                                          shinydashboard::menuItem("Search", tabName = "Search", icon = shiny::icon("table")),
                                                                          shinydashboard::menuItem("Data Source Quality", tabName = "sources", icon = shiny::icon("table")),
                                                                          shinydashboard::menuItem("Available Exposures", tabName = "exposureCohortsTab", icon = shiny::icon("table")),
                                                                          shinydashboard::menuItem("Available outcomes", tabName = "outcomeCohortsTab", icon = shiny::icon("table"))))


  shinydashboard::dashboardPage(shinydashboard::dashboardHeader(title = "REWARD: All Exposures by all outcomes"),
                                sidebar,
                                body)

}


#' @title
#' Launch the REWARD Shiny app report
#' @description
#' Launches a Shiny app for a given configuration file
#' @param appConfigPath path to configuration file. This is loaded in to the local environment with the appContext variable
#' @param exposureId exposure cohort id
#' @param outcomeId outcome cohort id
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