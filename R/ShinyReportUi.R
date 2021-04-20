reportUi <- function(request) {
  library(shiny, warn.conflicts = FALSE)
  library(shinyWidgets, warn.conflicts = FALSE)
  library(shinycssloaders, warn.conflicts = FALSE)

  fluidPage(
    tags$h1(textOutput("treatmentOutcomeStr")),
    tagList(
      tags$h2("Datasource Results and Meta-analysis"),
      metaAnalysisTableUi("metaTable"),
      tags$h2("Forest plot"),
      forestPlotUi("forestPlot"),
      tags$h2("Calibration plot"),
      calibrationPlotUi("calibrationPlot", figureTitle = "Figure 2.")
    ),
    title = "REWARD"
  )
}