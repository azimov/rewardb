reportUi <- function(request) {
  library(shiny, warn.conflicts = FALSE)
  library(shinyWidgets, warn.conflicts = FALSE)
  library(shinycssloaders, warn.conflicts = FALSE)
  library(shinymanager)

  resultsDisplayCondition <- "output.selectedCohorts == 'selected'"
  fluidPage(tags$h1("REWARD Dashboard - all exposures and outcomes (Prototype)"),
            tabsetPanel(tabPanel("Search",
                                 fluidRow(column(6,
                                                 selectizeInput("targetCohorts", label = "Drug exposures:", choices = NULL, multiple = FALSE, width = 500)),
                                          column(6,
                                                 selectizeInput("outcomeCohorts", label = "Disease outcomes:", choices = NULL, multiple = FALSE, width = 500))),
                                 conditionalPanel(condition = "output.selectedCohorts != 'selected'",
                                                  actionButton("selectCohorts", "Select")),
                                 conditionalPanel(condition = resultsDisplayCondition,
                                                  tagList(tags$h2(textOutput("treatmentOutcomeStr")),
                                                          tags$h3("Datasource Results and Meta-analysis"),
                                                          metaAnalysisTableUi("metaTable"),
                                                          tags$h3("Forest plot"),
                                                          forestPlotUi("forestPlot"))),
                                 textOutput("selectedCohorts")),
                        tabPanel("Available Exposure Cohorts",
                                 withSpinner(DT::dataTableOutput("exposureCohortsTable"))),
                        tabPanel("Available Outcome Cohorts",
                                 withSpinner(DT::dataTableOutput("outcomeCohortsTable")))),
            title = "REWARD")

}