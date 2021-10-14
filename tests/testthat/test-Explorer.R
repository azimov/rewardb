test_that("Build null dist settigns", {

  fullDbSetup()
  runDataBuild()
  runPreComputeNullDistributions(configFilePath)

  # Test all tables exist

  # Test concepts exist

  # Test
  expect_true(TRUE)
})