
timeToOutcomeServer <- function(id, dbConn, queryDb, appContext, selectedExposureOutcome) {
  server <- moduleServer(id, function(input, output, session) {
    getTimeToOutcomeStats <- reactive({
      s <- selectedExposureOutcome()
      treatment <- s$TARGET_COHORT_ID
      outcome <- s$OUTCOME_COHORT_ID

      data <- queryDb("
              SELECT
                ds.source_name,
                round(mean_time_to_outcome, 3) as mean,
                round(sd_time_to_outcome, 3) as sd,
                min_time_to_outcome as min,
                p10_time_to_outcome as p10,
                p25_time_to_outcome as p25,
                median_time_to_outcome as median,
                p75_time_to_outcome as p75,
                p90_time_to_outcome as p90,
                max_time_to_outcome as max

              FROM @schema.time_on_treatment tts
              LEFT JOIN @schema.data_source ds ON tts.source_id = ds.source_id
              WHERE exposure_id = @treatment AND outcome_id = @outcome",
                      treatment = treatment,
                      outcome = outcome
      )
      return(data)
    })

    output$timeToOutcomeStats <- DT::renderDataTable({
      data <- getTimeToOutcomeStats()

      output <- DT::datatable(
        data,
        colnames = c("Source", "Mean", "sd", "Min", "P10", "P25", "Median", "P75", "P90", "Max"),
        options = list(dom = 't', columnDefs = list(list(visible = FALSE, targets = c(0)))),
        caption = "Table: shows time to outcome distribution measaured in days between exposure and cohort across different databases."
      )
      return(output)
    })

    output$timeToOutcomeDist <- plotly::renderPlotly({
      dt <- getTimeToOutcomeStats()
      plot <- boxPlotDist(dt)
      return(plotly::ggplotly(plot))
    })
  })

  return(server)
}

timeOnTreatmentServer <- function(id, dbConn, queryDb, appContext, selectedExposureOutcome) {

  server <- moduleServer(id, function(input, output, session) {
    getTimeToTreatmentStats <- reactive({
      s <- selectedExposureOutcome()
      treatment <- s$TARGET_COHORT_ID
      outcome <- s$OUTCOME_COHORT_ID

      data <- queryDb("
          SELECT
            ds.source_name,
            round(mean_tx_time, 3) as mean,
            round(sd_tx_time, 3) as sd,
            min_tx_time as min,
            p10_tx_time as p10,
            p25_tx_time as p25,
            median_tx_time as median,
            p75_tx_time as p75,
            p90_tx_time as p90,
            max_tx_time as max
          FROM @schema.time_on_treatment tts
          LEFT JOIN @schema.data_source ds ON tts.source_id = ds.source_id
          WHERE exposure_id = @treatment AND outcome_id = @outcome",
                      treatment = treatment,
                      outcome = outcome
      )

      return(data)
    })


    output$timeToTreatmentStats <- DT::renderDataTable({
      data <- getTimeToTreatmentStats()

      output <- DT::datatable(
        data,
        colnames = c("Source", "Mean", "sd", "Min", "P10", "P25", "Median", "P75", "P90", "Max"),
        options = list(dom = 't', columnDefs = list(list(visible = FALSE, targets = c(0)))),
        caption = "Table: shows time on treatment distibution in days for cohort across databases."
      )
      return(output)
    })

    output$timeOnTreatmentDist <- plotly::renderPlotly({
      dt <- getTimeToTreatmentStats()
      plot <- boxPlotDist(dt)
      return(plotly::ggplotly(plot))
    })

  })

  return(server)
}

timeOnTreatmentUi <- function(id) {
  tagList(
    shinycssloaders::withSpinner(plotly::plotlyOutput(NS(id, "timeOnTreatmentDist"))),
    shinycssloaders::withSpinner(DT::dataTableOutput(NS(id, "timeToTreatmentStats")))
  )
}

timeToOutcomeUi <- function(id) {
  tagList(
   shinycssloaders::withSpinner(plotly::plotlyOutput(NS(id, "timeToOutcomeDist"))),
   shinycssloaders::withSpinner(DT::dataTableOutput(NS(id,"timeToOutcomeStats")))
  )
}
