#'@export
manhattanPlot <- function(dfFunc, xCol, yFunc) {
  df <- dfFunc()
  f <- list()
  x <- list(title = xCol(), titlefont = f)
  y <- list(title = yFunc, titlefont = f)
  plot <- plot_ly(df, x = ~df[, xCol()], y = ~get(yFunc), color = ~SOURCE_NAME, type = "scatter", mode = "markers",
                  text = ~paste(TARGET_COHORT_NAME, "\n", OUTCOME_COHORT_NAME))
  plot <- plot %>% layout(xaxis = x, yaxis = y)
  return(plot)
}

#'@export
forestPlot <- function(table) {

  label <- paste0("IRR= ", round(table$`RR` * 1, 2),
                  "; 95% CI= (", round(table$`LB_95`, 2), " - ", round(table$`UB_95`, 2), ")")
  plot <- ggplot2::ggplot(
    table,
    ggplot2::aes(
      y = factor(SOURCE_NAME, level = rev(SOURCE_NAME)),
      x = RR,
      xmin = LB_95,
      xmax = UB_95,
      label = label)
  ) +
    ggplot2::geom_pointrange() +
    ggplot2::geom_text(vjust = 0, nudge_y = 0.2) +
    ggplot2::geom_errorbarh(height = 0.1) +
    ggplot2::geom_vline(xintercept = 1.0, linetype = 2) +
    ggplot2::ylab("Database") +
    ggplot2::xlab("Relative Risk")
  return(plot)
}

#'@export
distPlot <- function(dfFunc, xCol, yFunc) {
  df <- dfFunc()
  plot <- ggplot(df) +
    aes_string(x = xCol(), y = yFunc) +
    geom_boxplot(aes(fill = SOURCE_NAME))
  return(plot)
}

#'@export
outcomeDistribution <- function(df, dfScores, target, outcome) {
  plot <- ggplot(df, aes(x = RR)) + geom_density(adjust = 3, trim = TRUE, alpha = 0.4, aes(fill = SOURCE_NAME))

  plot <- plot + geom_vline(data = dfScores, aes(xintercept = RR, color = SOURCE_NAME))
  return(ggplotly(plot))
}

#' Peform meta analysis on a dataframe of elements and append result to last row
#'@export
getMetaAnalysisData <- function(table) {
  table$I2 <- NA
  results <- meta::metainc(
    data = table,
    event.e = T_CASES,
    time.e = T_PT,
    event.c = C_CASES,
    time.c = C_PT,
    sm = "IRR",
    model.glmm = "UM.RS"
  )

  row <- data.frame(
    SOURCE_ID = -99,
    T_AT_RISK = sum(table$T_AT_RISK),
    T_PT = sum(table$T_PT),
    T_CASES = sum(table$T_CASES),
    C_AT_RISK = sum(table$C_AT_RISK),
    C_PT = sum(table$C_PT),
    C_CASES = sum(table$C_CASES),
    RR = exp(results$TE.random),
    LB_95 = exp(results$lower.random),
    UB_95 = exp(results$upper.random),
    P_VALUE = results$pval.random,
    I2 = results$I2
  )

  return(row)
}
