reportUi <- function(request) {
  library(shiny, warn.conflicts = FALSE)
  library(shinyWidgets, warn.conflicts = FALSE)
  library(shinycssloaders, warn.conflicts = FALSE)
  library(shinydashboard)
  library(shinymanager)

  resultsDisplayCondition <- "output.selectedCohorts == 'selected'"
  riskEstimatesPanel <- tabsetPanel(id = "searchResults",
                                    tabPanel("Risk Estimates",
                                             tags$h4("Datasource Results and Meta-analysis"),
                                             metaAnalysisTableUi("metaTable"),
                                             tags$h4("Forest plot"),
                                             forestPlotUi("forestPlot")),
                                    tabPanel("Outcome Calibration Plot",
                                             calibrationPlotUi("outcomeCalibrationPlot")))

  searchPanel <- tabItem("Search",
                         shinydashboard::box(width = 12,
                                             column(6,
                                                    selectizeInput("targetCohorts", label = "Drug exposures:", choices = NULL, multiple = FALSE, width = 500)),
                                             column(6,
                                                    selectizeInput("outcomeCohorts", label = "Disease outcomes:", choices = NULL, multiple = FALSE, width = 500))),
                         shinydashboard::box(width = 12,
                                             conditionalPanel(condition = "output.selectedCohorts != 'selected'",
                                                              actionButton("selectCohorts", "Select")),
                                             conditionalPanel(condition = resultsDisplayCondition,
                                                              tags$h3(textOutput("treatmentOutcomeStr")), riskEstimatesPanel),

                                             conditionalPanel(condition = "output.selectedCohorts != 'selected'",
                                                              textOutput("selectedCohorts"))))

  aboutTab <- tabItem("About",
                      shinydashboard::box(width = 12,
                                          h1("REWARD - all by all explorer"),
                                          p()))
  body <- dashboardBody(tabItems(aboutTab,
                                 searchPanel,
                                 tabItem("sources",
                                         shinydashboard::box(width = 12,
                                                             tags$h3("Registered data sources"),
                                                             withSpinner(DT::dataTableOutput("dataSourcesTable"))),
                                         shinydashboard::box(width = 12,
                                                             tags$h3("Data quality Indicator Pairs"),
                                                             withSpinner(gt::gt_output(outputId = "dataQaulityTable")))),
                                 tabItem("exposureCohortsTab",
                                         shinydashboard::box(width = 12, withSpinner(DT::dataTableOutput("exposureCohortsTable")))),
                                 tabItem("outcomeCohortsTab",
                                         shinydashboard::box(width = 12, withSpinner(DT::dataTableOutput("outcomeCohortsTable"))))))

  sidebar <- dashboardSidebar(sidebarMenu(
    menuItem("About", tabName = "About", icon = icon("list-alt")),
    menuItem("Search", tabName = "Search", icon = icon("table")),
    menuItem("Data Source Quality", tabName = "sources", icon = icon("table")),
    menuItem("Available Exposures", tabName = "exposureCohortsTab", icon = icon("table")),
    menuItem("Available outcomes", tabName = "outcomeCohortsTab", icon = icon("table"))))


  dashboardPage(dashboardHeader(title = "REWARD: All Exposures by all outcomes"),
                sidebar,
                body)

}