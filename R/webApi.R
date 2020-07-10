# Hit a web api REST endpoint
#'@export
getWebObject <- function(webApiUrl, resource, id) {
  definitionUrl <- URLencode(paste0(webApiUrl, "/", resource, "/", id))
  resp <- httr::GET(definitionUrl)
  content <- httr::content(resp, as = "text", encoding = "UTF-8")
  responseData <- RJSONIO::fromJSON(content)
  return(responseData)
}