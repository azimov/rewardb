library(shiny)
library(shinyWidgets)
library(shinydashboard)

scBenefitRisk <- c("none", "one", "most", "all")
# This hides the outcome exporues/result pairing
metaDisplayCondtion <- "typeof input.mainTable_rows_selected  !== 'undefined' && input.mainTable_rows_selected.length > 0"

mainResults <- box(
  width = 12,
  DT::dataTableOutput("mainTable"),
  conditionalPanel(
    condition = metaDisplayCondtion,
    box(
      width = 12,
      HTML(paste("<h4 id='mainR'>", textOutput("treatmentOutcomeStr"), "</h4>")),
      tabsetPanel(
        id = "tabsetPanelResults",
        tabPanel("Detailed results", DT::dataTableOutput("fullResultsTable")),
        tabPanel(
          "Forest plot",
          plotOutput("forestPlot", height = 800, hover = hoverOpts("plotHoverForestPlot")),
          div(strong("Figure 1."), "Forest plot of effect estimates from each database")
        )
      )
    )
  )
)

aboutTab <- fluidRow(
  box(
    width = 6,
    title=paste("About", appContext$name),
    div(HTML(paste("<p>", appContext$description, ".</p>"))),
    div(HTML("<p> Click the dashboard option to see the results.
          The sidebar options allow filtering of results based on risk and benift IRR thresholds. </p>")),
    downloadButton(
      "downloadData",
      "Download filtered results as a csv"
    )
  ),
  box(
    width = 6,
    title=paste("About REWARD-B"),
    p("These results are generated based developed by Observational Health Data Analytics (OHDA) at Janssen Research & Development.
      Unless otherwise state, all data remain the commerical property of Janssen Research & Development and partners.
    "),
    p("for more information about this dashboard please contact the REWARD-B team."),
    p("Observational Health Data Analytics:"),
    HTML("
      <ul>
        <li> Patrick Ryan (PRyan4@its.jnj.com) </li>
        <li> Christopher Knoll - (cknoll1@its.jnj.com) </li>
        <li> James Gilbert - (jgilber2@its.jnj.com) </li>
      </ul>"
     ),
     p("Epidemiology:"),
    HTML("
      <ul>
        <li> Soledad Cepeda (SCepeda@its.jnj.com) </li>
        <li> David Kern - (@its.jnj.com) </li>
        <li> Rachel Teneralli - (RTeneral@its.jnj.com) </li>
      </ul>
    ")
  )
)

body <- dashboardBody(
  tabItems(
    tabItem(
      tabName = "about",
      aboutTab
    ),
    tabItem(
      tabName = "",
      fluidRow(
        box(
          width = 6,
          title = "Filter Cohorts",
          collapsible = TRUE,
          uiOutput("targetCohorts"),
          uiOutput("outcomeCohorts"),
        ),
        mainResults
      )
    )
  )
)


sidebar <- dashboardSidebar(
  sidebarMenu(
    menuItem("About", tabName = "about", icon = icon("list-alt")),
    menuItem("Results", tabName = "", icon = icon("")),
    sliderInput("cutrange1", "Benefit Threshold:", min = 0.1, max = 0.9, step = 0.1, value = 0.5),
    sliderInput("cutrange2", "Risk Threshold:", min = 1.1, max = 2.5, step = 0.1, value = 2),
    checkboxInput("calibrated", "Threshold with empirically calibrated results", TRUE),
    pickerInput(
      "scBenefit",
      "Sources with self control benefit:",
      choices = scBenefitRisk,
      selected = "all",
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
    )
  )
)

appTitle <- paste("REWARD-B:", appContext$name)
# Put them together into a dashboardPage
ui <- dashboardPage(
  dashboardHeader(title = appTitle),
  sidebar,
  body
)


