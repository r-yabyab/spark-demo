val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))   
val rdd=spark.sparkContext.parallelize(dataSeq)

import spark.implicits._



val thing = sc.textFile("/path/to")

//
//
//
// Company Orders

case class CompanyOrders(OrderID: String, CompanyID: String, CompanyName: String, CompanyLocation: String, ComponentToBeManufactured: String, Quantity: Long, EstimatedCost: Double, OrderDate: String, DueDate: String, CompletionStatus: String, DeliveryStatus: String)


// COMPANY ORDERS... from .txt to RDD to DF
val companyOrdersRDD = sc.textFile("C:\\Users\\cayab\\spark-test\\resources\\CompanyOrders.txt")
// Parse the RDD and map it to the case class
val parsedCompanyOrdersRDD = companyOrdersRDD.map(line => {
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

val companyOrdersDF = parsedCompanyOrdersRDD.toDF()
companyOrdersDF.createOrReplaceTempView("Orders")

// to load from .txt straight to DF
val df = spark.read.option("delimiter", ",").csv("C:\\Users\\cayab\\spark-test\\resources\\CompanyOrders.txt")




// 
// 
// 
// COMPANY INFO

case class CompanyInfo(CompanyID: String, CompanyName: String, CompanyLocation: String, CompanyAddress: String, CompanyContact: Long, ProfitMargin: Double, EstablishedYear: Int)


val companyInfoRDD = sc.textFile("C:\\Users\\cayab\\spark-test\\resources\\CompanyInfo.txt")

val parsedCompanyInfoRDD = companyInfoRDD.map(line => {
    val fields = line.split(",")
    CompanyInfo(
      fields(0),
      fields(1),
      fields(2),
      fields(3),
      fields(4).toLong,
      fields(5).toDouble,
      fields(6).toInt
    )
  })

val companyInfoDF = parsedCompanyInfoRDD.toDF()
companyInfoDF.createOrReplaceTempView("Info")





// extra
rdd.take(5).foreach(println)





