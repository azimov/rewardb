#' @title
#' Forest plot
#' @description
#' Create a forest plot
#' @param table data.frame with columns RR, LB_95, UB_95
#' @return ggplot plot
forestPlot <- function(table) {
  table$SOURCE_ID <- as.character(table$SOURCE_ID)
  label <- paste0("IRR= ", round(table$`RR` * 1, 2),
                  "; 95% CI= (", round(table$`LB_95`, 2), " - ", round(table$`UB_95`, 2), ")")
  plot <- ggplot2::ggplot(
    table,
    ggplot2::aes(
      y = factor(SOURCE_NAME, level = rev(SOURCE_NAME)),
      x = RR,
      color = SOURCE_ID,
      xmin = LB_95,
      xmax = UB_95,
      label = label
    )
  ) +
    ggplot2::geom_pointrange() +
    ggplot2::geom_text(vjust = 0, nudge_y = 0.2, size = 3) +
    ggplot2::geom_errorbarh(height = 0.1) +
    ggplot2::geom_vline(xintercept = 1.0, linetype = 2) +
    ggplot2::ylab("Database") +
    ggplot2::scale_x_continuous(trans = "log2") +
    ggplot2::xlab("Relative Risk") +
    ggplot2::theme(text = ggplot2::element_text(size = 11), legend.position = "none")
  return(plot)
}

#' @title
#' Get Meta Analysis Data
#' @description
#' Peform meta analysis on a dataframe of elements and return row (e.g. to be appended with rbind
#' @param table expected data.frame containing fielsds: T_AT_RISK, T_PT, T_CASES, C_AT_RISK, C_PT, C_CASES, IRR
#'
#' @importFrom meta metainc
getMetaAnalysisData <- function(table) {
  meta::settings.meta('meta4')
  table$I2 <- NA
  results <- meta::metainc(data = table,
                           event.e = T_CASES,
                           time.e = T_PT,
                           event.c = C_CASES,
                           time.c = C_PT,
                           sm = "IRR",
                           model.glmm = "UM.RS")

  row <- data.frame(SOURCE_ID = -99,
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
                    I2 = results$I2)

  return(row)
}