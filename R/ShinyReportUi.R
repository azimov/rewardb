reportUi <- function(request) {
  library(shiny, warn.conflicts = FALSE)
  library(shinyWidgets, warn.conflicts = FALSE)
  library(shinycssloaders, warn.conflicts = FALSE)

  resultsDisplayCondition <- "output.selectedCohorts == 'selected'"
  fluidPage(tags$h1("REWARD Dashboard - all exposures and outcomes"),
            p("Note that any results that show significant implications for safety require reporting"),
            fluidRow(column(6,
                            selectizeInput("targetCohorts", label = "Drug exposures:", choices = NULL, multiple = FALSE, width = 500)),
                     column(6,
                            selectizeInput("outcomeCohorts", label = "Disease outcomes:", choices = NULL, multiple = FALSE, width = 500))),
            actionButton("selectCohorts", "Select"),
            conditionalPanel(condition = resultsDisplayCondition,
                             tagList(tags$h2(textOutput("treatmentOutcomeStr")),
                                     tags$h3("Datasource Results and Meta-analysis"),
                                     metaAnalysisTableUi("metaTable"),
                                     tags$h3("Forest plot"),
                                     forestPlotUi("forestPlot"))),
            title = "REWARD")
}