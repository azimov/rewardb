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

  label <- paste0("IRR= ", round(table$`RR` * 1, 2), "; 95% CI= (", round(table$`LB_95`, 2), " - ", round(table$`UB_95`, 2), ")")
  plot <- ggplot2::ggplot(data = table, aes(y = factor(SOURCE_NAME, level = rev(SOURCE_NAME)), x = RR, xmin = LB_95, xmax = UB_95, label = label)) +
    # Add data points and color them black
    geom_point(aes(shape = SOURCE_NAME, color = SOURCE_NAME, size = SOURCE_NAME)) +
    scale_shape_manual(values = c(16, 16, 16, 16, 16)) +
    scale_size_manual(values = c(3, 3, 3, 3, 3)) +
    scale_color_manual(values = c("black", "black", "black", "black", "black")) +
    geom_text(vjust = 0, nudge_y = 0.2) +
    geom_errorbarh(height = 0.1) +
    # Specify the limits of the x-axis and relabel it to something more meaningful
    xlab("Incident Rate Ratio (95% CI)") +
    ylab("Database") + # Add a vertical dashed line indicating an effect size of zero, for reference
    geom_vline(xintercept = 1, color = "black", linetype = "dashed") +
    scale_x_continuous(trans = log2_trans()) +
    theme_bw() +
    theme(
      panel.grid.major = element_blank(),
      panel.grid.minor = element_blank(),
      panel.border = element_blank(),
      axis.line = element_line(),
      text = element_text(family = "sans", size = 16),
      plot.title = element_text(size = 13, face = "bold.italic"), legend.position = "none"
    )
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
    SOURCE_NAME = "Meta Analysis",
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

  rbind(row, table)
}
