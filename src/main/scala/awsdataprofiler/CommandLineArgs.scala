package awsdataprofiler

case class CommandLineArgs(
  dbName: String = "",
  region: String = "",
  compExp: Boolean = false,
  statsPrefix: String = "DQP",
  s3BucketPrefix: String = "",
  profileUnsupportedTypes: Boolean = false,
  noOfBins: Int = 10,
  quantiles: Int = 10
)
