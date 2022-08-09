#' Ui for rewardb dashboard
#' @param request shiny request object
#' @importFrom gt gt_output
#' @import shiny
#' @import shinyWidgets
#' @importFrom shinycssloaders withSpinner
#' @import shinydashboard
dashboardUi <- function(request) {
  scBenefitRisk <- c("none", "one", "most", "all")
  # This hides the outcome exporues/result pairing
  metaDisplayCondtion <- "typeof input.mainTable_rows_selected  !== 'undefined' && input.mainTable_rows_selected.length > 0"

  filterBox <- shinydashboard::box(shinydashboard::box(shiny::selectizeInput("targetCohorts",
                                                                      label = "Drug exposures:",
                                                                      choices = NULL,
                                                                      multiple = TRUE),
                                                       shiny::selectizeInput("outcomeCohorts",
                                                                      label = "Disease outcomes:",
                                                                      choices = NULL,
                                                                      multiple = TRUE)),
                                   shinydashboard::box(shiny::selectizeInput("exposureClass",
                                                                             label = "Drug exposure classes:",
                                                                             choices = NULL,
                                                                             multiple = TRUE),
                                                       shinyWidgets::pickerInput("outcomeCohortTypes",
                                                                          "Outcome Cohort Types:",
                                                                          choices = c("ATLAS defined" = 3, "Inpatient" = 1, "Two diagnosis codes" = 0, "One diagnosis code" = 2),
                                                                          selected = c(),
                                                                          options = shinyWidgets::pickerOptions(
                                                                            actionsBox = TRUE,
                                                                            noneSelectedText = "Filter by subset"
                                                                          ),
                                                                          multiple = TRUE),
                                                       width = 6),
                                   width = 12,
                                   title = "Filter Cohorts",
                                   collapsible = TRUE)

  mainResults <- shinydashboard::box(
    shiny::fluidRow(
      shiny::column(2,
                    shiny::uiOutput("mainTablePage")
      ),
      shiny::column(6,
                    shiny::selectInput("mainTableSortBy",
                                       "Sort by column",
                                       choices = list(
                                         "Outcome id" = "OUTCOME_COHORT_ID",
                                         "Exposure id" = "TARGET_COHORT_ID",
                                         "Exposure name" = "TARGET_COHORT_NAME",
                                         "Outcome name" = "OUTCOME_COHORT_NAME",
                                         "I-squared" = "I2",
                                         "IRR" = "META_RR",
                                         "Sources with scc risk" = "RISK_COUNT",
                                         "Sources with scc benefit" = "BENEFIT_COUNT"),
                                       selected = "META_RR")
      ),
      shiny::column(2,
                    shiny::radioButtons("mainTableOrderAscending", "", c("Ascending" = "ASC", "Descending" = "DESC"))),
      shiny::column(2,
                    shiny::selectInput("mainTablePageSize",
                                       "Show per page",
                                       choices = c(5, 10, 15, 20, 25, 50, 100), selected = 10))),
    shinycssloaders::withSpinner(DT::dataTableOutput("mainTable")),
    shiny::hr(),
    shiny::fluidRow(
      shiny::column(4,
                    shiny::textOutput("mainTableCount"),
                    shiny::actionButton("mainTablePrevious", "Previous Page")
      ),
      shiny::column(6),
      shiny::column(2,
                    shiny::textOutput("mainTableNumPages"),
                    shiny::actionButton("mainTableNext", "Next Page"))),
    shiny::hr(),
    shiny::downloadButton("downloadFullTable", "Download"),
    width = 12)

  rPanel <- shiny::conditionalPanel(condition = metaDisplayCondtion,
                                    shinydashboard::box(
                                      shiny::HTML(paste("<h4 id='mainR'>", textOutput("treatmentOutcomeStr"), "</h4>")),
                                      shiny::tabsetPanel(
                                        id = "outcomeResultsTabs",
                                        shiny::tabPanel("Detailed results", metaAnalysisTableUi("metaTable")),
                                        shiny::tabPanel("Forest plot", forestPlotUi("forestPlot")),
                                        shiny::tabPanel("Calibration plot",
                                                        calibrationPlotUi("calibrationPlot",
                                                                          figureTitle = "Figure 2.")),
                                        shiny::tabPanel("Exposure concepts",
                                                        shiny::h4("Exposure Concepts"),
                                                        shinycssloaders::withSpinner(DT::dataTableOutput("selectedExposureConceptSet"))),
                                        shiny::tabPanel("Outcome concepts",
                                                        h4("Outcome Concepts"),
                                                        shinycssloaders::withSpinner(DT::dataTableOutput("selectedOutcomeConceptSet")))),
                                      width = 12))

  aboutTab <- shiny::fluidRow(
    shinydashboard::box(shiny::p("Mission:"),
                        shiny::includeHTML(system.file("static_html", "about_rewardb.html", package = "rewardb")),
                        width = 6,
                        title = paste("Real World Assessment and Research of Drug performance (REWARD)")),
    shinydashboard::box(shiny::includeHTML(system.file("static_html", "contact.html", package = "rewardb")),
                        width = 6,
                        title = paste("Contact")),
    shinydashboard::box(width = 6,
                        title = "Data sources",
                        shinycssloaders::withSpinner(gt::gt_output(outputId = "dataSourceTable"))),
    shinydashboard::box(shiny::p(appContext$description),
                        shiny::p("Click the dashboard option to see the results. The sidebar options allow filtering of results based on risk and benift IRR thresholds"),
                        shiny::downloadButton(
                          "downloadData",
                          "Download filtered results as a csv"),
                        shiny::downloadButton(
                          "downloadFullData",
                          "Download full results"),
                        width = 6,
                        title = paste("About this dashboard -", appContext$name)
    ),
    shinydashboard::box(shiny::p("Negative controls are used in this study to perform empirical calibration.
      These are selected automatically using the common evidence model. Indication mapping is used to filter results.
      Inidcations are based on ingredient labels, spontaneous adverse events reports and pubmed literature searches"),
                        shiny::downloadButton(
                          "downloadControls",
                          "Download Controls"
                        ),
                        shiny::downloadButton(
                          "downloadIndications",
                          "Download Indications"
                        ),
                        width = 6,
                        title = paste("Negative controls and indications")))

  body <- shinydashboard::dashboardBody(
    shinydashboard::tabItems(
      shinydashboard::tabItem(tabName = "about", aboutTab),
      shinydashboard::tabItem(tabName = "results", shiny::fluidRow(filterBox, mainResults, rPanel))
    )
  )

  sidebar <- shinydashboard::dashboardSidebar(
    shinydashboard::sidebarMenu(
      shinydashboard::menuItem("About", tabName = "about", icon = icon("rectangle-list")),
      shinydashboard::menuItem("Results", tabName = "results", icon = icon("table")),
      shiny::sliderInput("cutrange1", "Benefit Threshold:", min = 0.1, max = 0.9, step = 0.1, value = 0.5),
      shiny::sliderInput("cutrange2", "Risk Threshold:", min = 1.1, max = 2.5, step = 0.1, value = 2),
      shiny::sliderInput("pCut", "P-value cut off:", min = 0.0, max = 1.0, step = 0.01, value = 0.05),
      shiny::checkboxInput("calibrated", "Threshold with empirically calibrated IRR", TRUE),
      shiny::radioButtons("filterThreshold", "Threshold benefit by:", c("Data sources", "Meta analysis")),
      shiny::sliderInput("scBenefit",
                         "Minimum sources with self control benefit:",
                         min = 0,
                         max = 5,
                         step = 1,
                         value = 1),
      shiny::sliderInput("scRisk",
                         "Maximum sources with self control risk:",
                         min = 0,
                         max = 5,
                         step = 1,
                         value = 0),
      shinycssloaders::withSpinner(shiny::uiOutput("requiredDataSources")),
      shiny::bookmarkButton()))

  appTitle <- paste("REWARD:", appContext$name)
  # Put them together into a dashboardPage
  ui <- shinydashboard::dashboardPage(shinydashboard::dashboardHeader(title = appTitle),
                                      sidebar,
                                      body)
  return(ui)
}