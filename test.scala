val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))   
val rdd=spark.sparkContext.parallelize(dataSeq)

case class CompanyOrders(OrderID: String, CompanyID: String, CompanyName: String, CompanyLocation: String, ComponentToBeManufactured: String, Quantity: Long, EstimatedCost: Double, OrderDate: String, DueDate: String, CompletionStatus: String, DeliveryStatus: String)
case class CompanyInfo(CompanyID: String, CompanyName: String, CompanyLocation: String, CompanyAddress: String, CompanyContact: Long, ProfitMargin: Double, EstablishedYear: Int)


val thing = sc.textFile("/path/to")

// COMPANY ORDERS
val companyOrdersRDD = sc.textFile("C:\\Users\\cayab\\spark-test\\resources\\CompanyOrders.txt")
// Parse the RDD and map it to the case class
val parsedCompanyOrdersRDD = companyOrdersRDD
  .map(line => {
    // Split the line by comma
    val Array(orderID, companyID, companyName, companyLocation, component, quantity, estimatedCost, orderDate, dueDate, completionStatus, deliveryStatus) = line.split(",")
    
    // Return the CompanyOrders case class instance
    CompanyOrders(
      orderID, 
      companyID, 
      companyName, 
      companyLocation, 
      component, 
      quantity.toLong, 
      estimatedCost.toDouble, 
      orderDate, 
      dueDate, 
      completionStatus, 
      deliveryStatus
    )
  })

// COMPANY INFO
val companyInfoRDD = sc.textFile("C:\\Users\\cayab\\spark-test\\resources\\CompanyInfo.txt")




