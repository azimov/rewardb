reportUi <- function(request) {
  library(shiny, warn.conflicts = FALSE)
  library(shinyWidgets, warn.conflicts = FALSE)
  library(shinycssloaders, warn.conflicts = FALSE)
  library(shinymanager)

  resultsDisplayCondition <- "output.selectedCohorts == 'selected'"
  riskEstimatesPanel <- tabsetPanel(id = "searchResults",
                                    tabPanel("Risk Estimates",
                                             tags$h3("Datasource Results and Meta-analysis"),
                                             metaAnalysisTableUi("metaTable"),
                                             tags$h3("Forest plot"),
                                             forestPlotUi("forestPlot")))

  searchPanel <- tabPanel("Search",
                          fluidRow(column(6,
                                          selectizeInput("targetCohorts", label = "Drug exposures:", choices = NULL, multiple = FALSE, width = 500)),
                                   column(6,
                                          selectizeInput("outcomeCohorts", label = "Disease outcomes:", choices = NULL, multiple = FALSE, width = 500))),
                          conditionalPanel(condition = "output.selectedCohorts != 'selected'",
                                           actionButton("selectCohorts", "Select")),
                          conditionalPanel(condition = resultsDisplayCondition,
                                           tags$h2(textOutput("treatmentOutcomeStr")), riskEstimatesPanel),
                          textOutput("selectedCohorts"))

  fluidPage(tags$h1("REWARD Dashboard - all exposures and outcomes (Prototype)"),
            tabsetPanel(searchPanel,
                        tabPanel("Available Data Sources",
                                 withSpinner(DT::dataTableOutput("dataSourcesTable"))),
                        tabPanel("Available Exposure Cohorts",
                                 withSpinner(DT::dataTableOutput("exposureCohortsTable"))),
                        tabPanel("Available Outcome Cohorts",
                                 withSpinner(DT::dataTableOutput("outcomeCohortsTable")))),
            title = "REWARD")

}