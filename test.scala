val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))   
val rdd=spark.sparkContext.parallelize(dataSeq)

import spark.implicits._

case class CompanyOrders(OrderID: String, CompanyID: String, CompanyName: String, CompanyLocation: String, ComponentToBeManufactured: String, Quantity: Long, EstimatedCost: Double, OrderDate: String, DueDate: String, CompletionStatus: String, DeliveryStatus: String)
case class CompanyInfo(CompanyID: String, CompanyName: String, CompanyLocation: String, CompanyAddress: String, CompanyContact: Long, ProfitMargin: Double, EstablishedYear: Int)


val thing = sc.textFile("/path/to")

// COMPANY ORDERS... from .txt to RDD to DF
val companyOrdersRDD = sc.textFile("C:\\Users\\cayab\\spark-test\\resources\\CompanyOrders.txt")
// Parse the RDD and map it to the case class
val parsedCompanyOrdersRDD = rdd.map(line => {
  val fields = line.split(",")
  CompanyOrders(
    fields(0), // OrderID
    fields(1), // CompanyID
    fields(2), // CompanyName
    fields(3), // CompanyLocation
    fields(4), // ComponentToBeManufactured
    fields(5).toLong, // Quantity
    fields(6).toDouble, // EstimatedCost
    fields(7), // OrderDate
    fields(8), // DueDate
    fields(9), // CompletionStatus
    fields(10) // DeliveryStatus
  )
})

parsedCompanyOrdersRDD.take(5).foreach(println)

// to load from .txt straight to DF
val df = spark.read.option("delimiter", ",").csv("C:\\Users\\cayab\\spark-test\\resources\\CompanyOrders.txt")

// COMPANY INFO
val companyInfoRDD = sc.textFile("C:\\Users\\cayab\\spark-test\\resources\\CompanyInfo.txt")
val parsedCompanyInfoRDD = companyInfoRDD.map(line => {

    val Array(companyID, companyName, companyLocation, companyAddress, companyContact, profitMargin, establishedYear) = line.split(",")

    CompanyInfo(
        companyID, companyName, companyLocation, companyAddress, companyContact.toLong, profitMargin.toDouble, establishedYear.toInt
    )

})

// this one works for some reason, but the one above doesn't
val parsedCompanyInfoRDD = companyInfoRDD
  .filter(line => line != header)  // Remove the header if it exists
  .map(line => {
    val Array(companyID, companyName, companyLocation, companyAddress, companyContact, profitMargin, establishedYear) = line.split(",")
    CompanyInfo(
      companyID,
      companyName,
      companyLocation,
      companyAddress,
      companyContact.toLong,
      profitMargin.toDouble,
      establishedYear.toInt
    )
  })

parsedCompanyInfoRDD.take(5).foreach(println)


val companyInfoDF = parsedCompanyInfoRDD.toDF()
val companyOrdersDF = parsedCompanyOrdersRDD.toDF(
  "OrderID", 
  "CompanyID", 
  "CompanyName", 
  "CompanyLocation", 
  "ComponentToBeManufactured", 
  "Quantity", 
  "EstimatedCost", 
  "OrderDate", 
  "DueDate", 
  "CompletionStatus", 
  "DeliveryStatus"
)

