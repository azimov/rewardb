dashboardUi  <- function (request) {
  library(shiny)
  library(shinyWidgets)
  library(shinydashboard)
  scBenefitRisk <- c("none", "one", "most", "all")
  # This hides the outcome exporues/result pairing
  metaDisplayCondtion <- "typeof input.mainTable_rows_selected  !== 'undefined' && input.mainTable_rows_selected.length > 0"

  mainResults <- box(
    DT::dataTableOutput("mainTable"),
    width = 12
  )

  rPanel <- conditionalPanel(
      condition = metaDisplayCondtion,
      box(
        HTML(paste("<h4 id='mainR'>", textOutput("treatmentOutcomeStr"), "</h4>")),
        tabsetPanel(
          id = "tabsetPanelResults",
          tabPanel("Detailed results", DT::dataTableOutput("fullResultsTable")),
          tabPanel(
            "Forest plot",
            plotOutput("forestPlot", height = 800, hover = hoverOpts("plotHoverForestPlot")),
            div(strong("Figure 1."), "Forest plot of effect estimates from each database")
          )
        ),
        width = 12
      )
    )

  aboutTab <- fluidRow(
    box(
      p(appContext$description),
      p("Click the dashboard option to see the results. The sidebar options allow filtering of results based on risk and benift IRR thresholds"),
      downloadButton(
        "downloadData",
        "Download filtered results as a csv"
      ),
      width = 6,
      title=paste("About", appContext$name)
    ),
    box(
      includeHTML(system.file("HTML", "about_rewardb.html", package = "rewardb")),
      width = 6,
      title=paste("About REWARD-B")
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
              uiOutput("targetCohorts"),
              uiOutput("outcomeCohorts")
            ),
            box(
              pickerInput(
              "outcomeCohortTypes",
              "Outcome Cohort Types:",
                choices = c("ATLAS", "Inpatient", "Two diagnosis codes"),
                selected = c("ATLAS", "Inpatient", "Two diagnosis codes"),
                options = shinyWidgets::pickerOptions(actionsBox = TRUE),
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
      checkboxInput("calibrated", "Threshold with empirically calibrated results", TRUE),
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

