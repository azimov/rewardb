manhattanPlot <- function(dfFunc, xCol, yFunc) {
  df <- dfFunc()
  f <- list()
  x <- list(
    title = xCol(),
    titlefont = f
  )
  y <- list(
    title = yFunc,
    titlefont = f
  )
  plot <- plot_ly(df, x = ~df[, xCol()], y = ~get(yFunc), color = ~SOURCE_NAME, type = 'scatter', mode = 'markers',
                  text = ~paste(TARGET_COHORT_NAME, "\n", OUTCOME_COHORT_NAME))
  plot <- plot %>% layout(xaxis = x, yaxis = y)
  return(plot)
}

forestPlot <- function(data) {
  # plot <- metainc(event.e=T_CASES, time.e=T_PT, event.c=C_CASES, time.c=C_PT, studlab=SOURCE_NAME, data=table5, sm="IRR", method="MH")
  label <- paste0("IRR= ", round(data$RR * 1.0, 2), "; 95% CI= (", round(data$LB_95, 2), " - ", round(data$UB_95, 2), ")")
  plot <- ggplot2::ggplot(data = data, aes(y = SOURCE_NAME, x = data$RR, xmin = data$LB_95, xmax = data$UB_95, label = label)) +
    #Add data points and color them black
    ggtitle(paste0("Association between ", data$TARGET_COHORT_NAME, " and ", data$OUTCOME_COHORT_NAME)) +
    # geom_point(color = 'black')+
    geom_point(aes(shape = SOURCE_NAME, color = SOURCE_NAME, size = SOURCE_NAME)) +
    scale_shape_manual(values = c(18, 16, 16, 16, 16)) +
    scale_size_manual(values = c(10, 3, 3, 3, 3)) +
    scale_color_manual(values = c('darkgreen', 'black', 'black', 'black', 'black')) +
    geom_text(vjust = 0, nudge_y = 0.2) +
    geom_errorbarh(height = .1) +
    #Specify the limits of the x-axis and relabel it to something more meaningful
    xlab('Incident Rate Ratio (95% CI)') +
    ylab('Database') +
    #Add a vertical dashed line indicating an effect size of zero, for reference
    geom_vline(xintercept = 1.0, color = 'black', linetype = 'dashed') +
    scale_x_continuous(trans = log2_trans()) +
    theme_bw() +
    theme(panel.grid.major = element_blank(),
          panel.grid.minor = element_blank(),
          panel.border = element_blank(),
          axis.line = element_line(),
          text = element_text(family = 'sans', size = 16),
          plot.title = element_text(size = 13, face = "bold.italic"),
          legend.position = 'none')
  return(plot)
}

distPlot <- function(dfFunc, xCol, yFunc) {
  df <- dfFunc()
  plot <- ggplot(df) +
    aes_string(x = xCol(), y = yFunc) +
    geom_boxplot(aes(fill=SOURCE_NAME))
  return(plot)
}

outcomeDistribution <- function(df, dfScores, target, outcome) {
  plot <- ggplot(df, aes(x=RR)) + geom_density(adjust=3, trim=TRUE, alpha = 0.4, aes(fill=SOURCE_NAME))

  plot <- plot + geom_vline(data=dfScores, aes(xintercept=RR, color=SOURCE_NAME))
  return(ggplotly(plot))
}