#' Ui for rewardb dashboard
#' @param request shiny request object
dashboardUi <- function(request) {
  library(shiny, warn.conflicts = FALSE)
  library(shinyWidgets, warn.conflicts = FALSE)
  library(shinydashboard, warn.conflicts = FALSE)
  library(shinycssloaders, warn.conflicts = FALSE)

  scBenefitRisk <- c("none", "one", "most", "all")
  # This hides the outcome exporues/result pairing
  metaDisplayCondtion <- "typeof input.mainTable_rows_selected  !== 'undefined' && input.mainTable_rows_selected.length > 0"

  filterBox <- box(
    box(
      selectizeInput("targetCohorts", label = "Drug exposures:", choices = NULL, multiple = TRUE),
      selectizeInput("outcomeCohorts", label = "Disease outcomes:", choices = NULL, multiple = TRUE)
    ),
    box(
      selectizeInput("exposureClass", label = "Drug exposure classes:", choices = NULL, multiple = TRUE),
      pickerInput(
        "outcomeCohortTypes",
        "Outcome Cohort Types:",
        choices = c("ATLAS defined", "Inpatient", "Two diagnosis codes"),
        selected = c(),
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE,
          noneSelectedText = "Filter by subset"
        ),
        multiple = TRUE
      ),
      checkboxInput("excludeIndications", "Exclude any mapped associations", TRUE),
      p("Mapped assocations includes drug label indications and contra-indications, spontaneous reports, and MESH literature searches."),
      width = 6
    ),
    width = 12,
    title = "Filter Cohorts",
    collapsible = TRUE
  )

  mainResults <- box(
    fluidRow(
      column(2,
             uiOutput("mainTablePage")
      ),
      column(6,
             selectInput("mainTableSortBy",
                         "Sort by column",
                         choices = list(
                           "Outcome id" = "OUTCOME_COHORT_ID",
                           "Exposure id" = "TARGET_COHORT_ID",
                           "Exposure name" = "TARGET_COHORT_NAME",
                           "Outcome name" = "OUTCOME_COHORT_NAME",
                           "I-squared" = "I2",
                           "IRR" = "META_RR",
                           "Sources with scc risk" = "RISK_COUNT",
                           "Sources with scc benefit" = "BENEFIT_COUNT"
                         )
             )
      ),
      column(2,
             radioButtons("mainTableOrderAscending", "", c("Ascending" = "ASC", "Descending" = "DESC"))
      ),
      column(2,
             selectInput("mainTablePageSize", "Show per page", choices = c(5, 10, 15, 20, 25, 50, 100), selected = 10)
      )
    ),
    withSpinner(DT::dataTableOutput("mainTable")),
    hr(),
    fluidRow(
      column(4,
             textOutput("mainTableCount")
      ),
      column(6),
      column(2,
             textOutput("mainTableNumPages")
      )
    ),
    hr(),
    downloadButton("downloadFullTable", "Download"),
    width = 12
  )

  rPanel <- conditionalPanel(
    condition = metaDisplayCondtion,
    box(
      HTML(paste("<h4 id='mainR'>", textOutput("treatmentOutcomeStr"), "</h4>")),
      tabsetPanel(
        id = "outcomeResultsTabs",
        tabPanel(
          "Detailed results",
          metaAnalysisTableUi("metaTable")
        ),
        tabPanel(
          "Forest plot",
          forestPlotUi("forestPlot")
        ),
        tabPanel(
          "Calibration plot",
          calibrationPlotUi("calibrationPlot", figureTitle = "Figure 2.")
        )
      ),
      width = 12
    )
  )

  aboutTab <- fluidRow(
    box(
      p("Mission:"),
      includeHTML(system.file("static_html", "about_rewardb.html", package = "rewardb")),
      width = 6,
      title = paste("Real World Assessment and Research of Drug performance (REWARD)")
    ),
    box(
      includeHTML(system.file("static_html", "contact.html", package = "rewardb")),
      width = 6,
      title = paste("Contact")
    ),
    box(
      p(appContext$description),
      p("Click the dashboard option to see the results. The sidebar options allow filtering of results based on risk and benift IRR thresholds"),
      downloadButton(
        "downloadData",
        "Download filtered results as a csv"
      ),
      width = 6,
      title = paste("About this dashboard -", appContext$name)
    ),
    box(
      p("Negative controls are used in this study to perform empirical calibration.
      These are selected automatically using the common evidence model. Indication mapping is used to filter results.
      Inidcations are based on ingredient labels, spontaneous adverse events reports and pubmed literature searches"),
      downloadButton(
        "downloadControls",
        "Download Controls"
      ),
      downloadButton(
        "downloadIndications",
        "Download Indications"
      ),
      width = 6,
      title = paste("Negative controls and indications")
    )
  )

  body <- dashboardBody(
    tabItems(
      tabItem(
        tabName = "about",
        aboutTab
      ),
      tabItem(
        tabName = "results",
        fluidRow(
          filterBox,
          mainResults,
          rPanel
        )
      )
    )
  )


  sidebar <- dashboardSidebar(
    sidebarMenu(
      menuItem("About", tabName = "about", icon = icon("list-alt")),
      menuItem("Results", tabName = "results", icon = icon("table")),
      sliderInput("cutrange1", "Benefit Threshold:", min = 0.1, max = 0.9, step = 0.1, value = 0.5),
      sliderInput("cutrange2", "Risk Threshold:", min = 1.1, max = 2.5, step = 0.1, value = 2),
      sliderInput("pCut", "P-value cut off:", min = 0.0, max = 1.0, step = 0.01, value = 0.05),
      checkboxInput("calibrated", "Threshold with empirically calibrated IRR", TRUE),
      radioButtons("filterThreshold", "Threshold benefit by:", c("Data sources", "Meta analysis")),
      pickerInput(
        "scBenefit",
        "Sources with self control benefit:",
        choices = scBenefitRisk,
        selected = c("all", "most"),
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
      bookmarkButton()
    )
  )

  appTitle <- paste("REWARD:", appContext$name)
  # Put them together into a dashboardPage
  ui <- dashboardPage(
    dashboardHeader(title = appTitle),
    sidebar,
    body
  )
  return(ui)
}

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