library(shiny)
library(shinyWidgets)

filterSql <- "SELECT DISTINCT(OUTCOME_COHORT_ID), OUTCOME_COHORT_NAME FROM full_results"
outcomes <- DatabaseConnector::renderTranslateQuerySql(conn, filterSql)

filterSql <- "SELECT DISTINCT(TARGET_COHORT_ID), TARGET_COHORT_NAME FROM full_results"
treatments <- DatabaseConnector::renderTranslateQuerySql(conn, filterSql)

exposureClassesSql <- "SELECT DISTINCT(EXPOSURE_CLASS) FROM TREATMENT_CLASSES ORDER BY EXPOSURE_CLASS"
exposureClasses <- DatabaseConnector::renderTranslateQuerySql(conn, exposureClassesSql)

scBenefits <- c("none", "one", "most", "all")
scRisks <- c("none", "one", "most", "all")

### UI Script ###
ui <- fluidPage(
    titlePanel(HTML(paste("<h1>REWARD-B Dashboard</h1>")), windowTitle = "All-by-all analysis"),
    fluidRow(
        column(3,
                setSliderColor(c("#DDDDDD"), c(1)),
                shiny::sliderInput("cutrange", "Relative Risk Effect size range:", min = 0.1, max = 2.5, value = c(0.5, 2.0)),
                shiny::actionButton("querySql", "Update Results"),
                uiOutput("selectTreatement"),
                uiOutput("selectOutcome"),
                shinyWidgets::pickerInput("exposureClasses", "Exposure Classes:",
                                        choices = exposureClasses$EXPOSURE_CLASS,
                                        selected = exposureClasses$EXPOSURE_CLASS,
                                        options = shinyWidgets::pickerOptions(actionsBox = TRUE, liveSearch = TRUE),
                                        multiple = TRUE),
                shinyWidgets::pickerInput("scBenefit", "Sources with self control benefit:",
                                        choices = scBenefits, selected = "all",
                                        options = shinyWidgets::pickerOptions(actionsBox = TRUE),
                                        multiple = TRUE),
                shinyWidgets::pickerInput("scRisk", "Sources with self control risk:",
                                        choices = scRisks, selected = "none",
                                        options = shinyWidgets::pickerOptions(actionsBox = TRUE),
                                        multiple = TRUE),
                downloadButton("downloadtable", "Download file", style = "display: block; margin: 0 auto; width: 230px;color: blue;")
        ),

        column(9,

                tabsetPanel(id = "mainPanel",
                            tabPanel("Main Results",
                                    DT::dataTableOutput("mainTable"),
                                    conditionalPanel(condition = "typeof input.mainTable_rows_selected  !== 'undefined' && input.mainTable_rows_selected.length > 0",
                                                    HTML(paste("<h4 id='mainR'>", textOutput("treatmentOutcomeStr"), "</h4>")),
                                                    tabsetPanel(id = "tabsetPanelResults",
                                                                tabPanel("Detailed results", DT::dataTableOutput("fullResultsTable")),
                                                                tabPanel("Forest plot",
                                                                        plotOutput("forestPlot", height = 500, hover = hoverOpts("plotHoverForestPlot")),
                                                                        div(strong("Figure 1."), "Forest plot of effect estimates from each database")
                                                                )
                                                    )
                                    )
                            ),
                            tabPanel("Plots",
                                    HTML("<h4> Plot configuration </h4>"),
                                    column(9,
                                            shinyWidgets::pickerInput("yFunc", "Y Function",
                                                                    choices = c("RR", "log(RR)", "1/RR",
                                                                                "-1 * RR", "-1 * log(RR)",
                                                                                "P_VALUE", "LB_95", "UB_95"
                                                                    )
                                            ),
                                            uiOutput("selectMx"),
                                            uiOutput("selectDataSource")
                                    ),
                                    tabsetPanel(id = "plotPanel",
                                                tabPanel("Manhattan plot",
                                                        plotly::plotlyOutput("mplot"),
                                                        div(strong("Figure 2."), "Plot of effect estimates")
                                                ),
                                                tabPanel("Dist plot",
                                                        plotOutput("distPlot", height = 500),
                                                        div(strong("Figure 3."), "Distribution of effect estimates")
                                                )
                                    )
                            )

                )
        )
    )
)