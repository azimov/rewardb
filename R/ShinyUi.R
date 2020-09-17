#' Ui for rewardb dashboard
#' @param request shiny request object
dashboardUi  <- function (request) {
  library(shiny, warn.conflicts=FALSE)
  library(shinyWidgets, warn.conflicts=FALSE)
  library(shinydashboard, warn.conflicts=FALSE)
  library(shinycssloaders, warn.conflicts=FALSE)

  scBenefitRisk <- c("none", "one", "most", "all")
  # This hides the outcome exporues/result pairing
  metaDisplayCondtion <- "typeof input.mainTable_rows_selected  !== 'undefined' && input.mainTable_rows_selected.length > 0"

  mainResults <- box(
    withSpinner(DT::dataTableOutput("mainTable")),
    downloadButton("downloadFullTable", "Download"),
    width = 12
  )

  rPanel <- conditionalPanel(
      condition = metaDisplayCondtion,
      box(
        HTML(paste("<h4 id='mainR'>", textOutput("treatmentOutcomeStr"), "</h4>")),
        tabsetPanel(
          id = "tabsetPanelResults",
        tabPanel(
          "Detailed results",
          withSpinner(DT::dataTableOutput("fullResultsTable")),
            downloadButton("downloadSubTable", "Download")
          ),
          tabPanel(
            "Forest plot",
            withSpinner(plotly::plotlyOutput("forestPlot", height = 800)),
            div(strong("Figure 1."), "Forest plot of effect estimates from each database")
          ),
          tabPanel(
            "Calibration plot",
            withSpinner(plotly::plotlyOutput("calibrationPlot", height = 800)),
            div(
              strong("Figure 2."),
              paste("Plot of calibration of effect estimates. Blue indicates controls, yellow diamonds indicate uncalibrated effect estimates")
            )
          )
        ),
        width = 12
      )
    )

  aboutTab <- fluidRow(
    box(
      p("Mission:"),
      includeHTML(system.file("html", "about_rewardb.html", package = "rewardb")),
      width = 6,
      title=paste("Real World Assessment and Research of Drug Benefits (REWARD-B)")
    ),
    box(
      p(appContext$description),
      p("Click the dashboard option to see the results. The sidebar options allow filtering of results based on risk and benift IRR thresholds"),
      downloadButton(
        "downloadData",
        "Download filtered results as a csv"
      ),
      width = 6,
      title=paste("About this dashboard -", appContext$name)
    ),
    box(
      includeHTML(system.file("html", "contact.html", package = "rewardb")),
      width = 6,
      title=paste("Contact")
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
          box(
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
              checkboxInput("excludeIndications", "Exclude any mapped indications", TRUE),
              width = 6
            ),
            width = 12,
            title = "Filter Cohorts",
            collapsible = TRUE
          ),
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

  appTitle <- paste("REWARD-B:", appContext$name)
  # Put them together into a dashboardPage
  ui <- dashboardPage(
    dashboardHeader(title = appTitle),
    sidebar,
    body
  )
  return(ui)
}

