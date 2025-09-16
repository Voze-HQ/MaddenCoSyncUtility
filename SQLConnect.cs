using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.Odbc;
using Npgsql;


namespace MaddenCoSyncUtility
{
    class SQLConnect
    {

        public System.Data.SqlClient.SqlConnection TNSQLConnection()
        {
            return TNSQLConnection("Telenotes");
        }
        public NpgsqlConnection TNPostgresConnection()
        {
            // Default connection values
            string host = "integration-2-reader.cfvu1alal4rz.us-west-1.rds.amazonaws.com";  // or your server name
            int port = 5432;            // default PostgreSQL port
            string username = "synapse_user"; // your default username
            string password = "iePwF7cdduGJUgbueDtMKT6V3ThAGQbuRWnqQwp3emii6eNPP3acQdyrPxDrAEyE"; // your default password
            string database = "integrationdb"; // your default database

            // You can modify these based on clientId if needed
            // Example:
            // if (clientId == 1) { database = "client1db"; }

            var connString = $"Host={host};Port={port};Username={username};Password={password};Database={database};";

            return new NpgsqlConnection(connString);
        }
        public object Coalesce(object Value, object ValueIfNull)
        {
            if (Value != null && Value != System.DBNull.Value)
            {
                return Value;
            }
            else
            {
                return ValueIfNull;
            }
        }
        
        public System.Data.Odbc.OdbcConnection TNODBCConnection(int clientId)
        {
            string sConnection = "";
            //"Dsn=Snider ODBC;uid=U220READTN;pwd=zt964r";
            if (clientId == 417)
            {
                sConnection = "Dsn=Snider ODBC;uid=MCODB220VZ;pwd=sn1d3r220v";
            }
            if (clientId == 1148)
            {
                sConnection = "Dsn=CMC Tire;uid=U676READO;pwd=CMC676z";
            }
            if (clientId == 1334)
            {
                sConnection = "Dsn=Commercial tire Services;uid=MCODB559VZ;pwd=COM559VOZE";
            }
            if (clientId == 1366)
            {
                sConnection = "Dsn=Conlan Tire;uid=MCODB695VZ;pwd=V695OZE25";
            }
            if (clientId == 1387)
            {
                sConnection = "Dsn= Alma Tire Service;uid=MCODB703VZ;pwd=VZ703ALMTR";
            }
            if (clientId == 385)
            {
                sConnection = "Dsn= Bauer Built 385;uid=MCODB539VZ;pwd=BAUER539VZ";
            }
            if (clientId == 1217)
            {
                sConnection = "Dsn=Purcell 1217;uid=MCODB257VZ;pwd=PTBTS257YQ";
            }

            System.Data.Odbc.OdbcConnectionStringBuilder oODBCSB = new System.Data.Odbc.OdbcConnectionStringBuilder(sConnection);


            System.Data.Odbc.OdbcConnection conn = new System.Data.Odbc.OdbcConnection(oODBCSB.ConnectionString);

            return conn;
        }
        public System.Data.SqlClient.SqlConnection TNSQLConnection(string InitialCatalog)
        {
            System.Data.SqlClient.SqlConnectionStringBuilder oSQLSB = new SqlConnectionStringBuilder();

            //oSQLSB.UserID = "sa";
            //oSQLSB.Password = "[NHY^6tgb]";
            //oSQLSB.DataSource = @"TNDevServer\TNSQLDev";


            oSQLSB.UserID = "WebUSR";
            oSQLSB.Password = "7Ae@9mF52";
            oSQLSB.DataSource = "tndb01.database.windows.net,1433";

            oSQLSB.PersistSecurityInfo = true;
            oSQLSB.MultipleActiveResultSets = true;
            oSQLSB.PacketSize = 4092;
            oSQLSB.ConnectTimeout = 3600;



            oSQLSB.InitialCatalog = InitialCatalog;

            System.Data.SqlClient.SqlConnection conn = new System.Data.SqlClient.SqlConnection(oSQLSB.ConnectionString.ToString());

            return conn;
        }

        public System.Data.SqlClient.SqlConnection TNSQLLinkedConnection(string InitialCatalog)
        {
            System.Data.SqlClient.SqlConnectionStringBuilder oSQLSB = new SqlConnectionStringBuilder();

            //oSQLSB.UserID = "sa";
            //oSQLSB.Password = "[NHY^6tgb]";
            //oSQLSB.DataSource = @"TNDevServer\TNSQLDev";


            oSQLSB.UserID = "integrations";
            oSQLSB.Password = "*S0wi1[L %`msI";
            oSQLSB.DataSource = @"TNDEVSRV-ONPREM";

            oSQLSB.PersistSecurityInfo = true;
            oSQLSB.MultipleActiveResultSets = true;
            oSQLSB.PacketSize = 4092;
            oSQLSB.ConnectTimeout = 0;



            oSQLSB.InitialCatalog = InitialCatalog;

            System.Data.SqlClient.SqlConnection conn = new System.Data.SqlClient.SqlConnection(oSQLSB.ConnectionString.ToString());

            return conn;
        }




        public bool SQLBulkCopy(DataTable oSource, string Catalog)
        {
            using (SqlConnection connection = TNSQLConnection(Catalog))
            {
                connection.Open();
                //connection.se = 100000;

                using (SqlBulkCopy BulkCopy = new SqlBulkCopy(connection.ConnectionString))
                {
                    BulkCopy.BulkCopyTimeout = 600;
                    BulkCopy.ColumnMappings.Add("IDT", "InvoiceDate");
                    BulkCopy.ColumnMappings.Add("STR", "STR");
                    BulkCopy.ColumnMappings.Add("LCT", "LCT");
                    BulkCopy.ColumnMappings.Add("TCNDATA", "ProductName");
                    BulkCopy.ColumnMappings.Add("TCNDATA2", "Description");
                    BulkCopy.ColumnMappings.Add("UNITS", "Qty");
                    BulkCopy.ColumnMappings.Add("TOTALSALES", "Price");
                    BulkCopy.ColumnMappings.Add("FEDTAX", "Fet");
                    BulkCopy.ColumnMappings.Add("SALES", "Extension");
                    BulkCopy.ColumnMappings.Add("ISADD", "IsAdd");
                    BulkCopy.ColumnMappings.Add("REPCOST", "Cost");
                    BulkCopy.ColumnMappings.Add("PROFIT", "Profit");
                    BulkCopy.ColumnMappings.Add("TSSMS", "SalesPersonID");
                    BulkCopy.ColumnMappings.Add("TSSMC", "tssmc");
                    BulkCopy.ColumnMappings.Add("PRODUCTCODE", "ProductID");
                    BulkCopy.ColumnMappings.Add("CUSTOMERID", "CustomerID");
                    BulkCopy.ColumnMappings.Add("CLIENTID", "clientid");
                    BulkCopy.ColumnMappings.Add("TSYP2", "MDINVDT");
                    BulkCopy.ColumnMappings.Add("INDSALES", "IndSales");
                    BulkCopy.ColumnMappings.Add("MECHSALES", "MechSales");
                    BulkCopy.ColumnMappings.Add("TSST", "TSST");
                    BulkCopy.ColumnMappings.Add("TSCC", "TSCC");
                    BulkCopy.ColumnMappings.Add("CUUSFLAG6", "CUUSFLAG6");
                    BulkCopy.ColumnMappings.Add("CUNAME", "customerName");
                    BulkCopy.ColumnMappings.Add("TCNKEY", "salespersonKey");
                    BulkCopy.ColumnMappings.Add("dsaid", "dsaid");
                    BulkCopy.ColumnMappings.Add("parentid", "parentid");
                    BulkCopy.ColumnMappings.Add("parentname", "parentname");
                    BulkCopy.ColumnMappings.Add("TSCOSACT", "costAct");
                    BulkCopy.ColumnMappings.Add("custcity", "custcity");
                    BulkCopy.ColumnMappings.Add("custaddress", "custaddress");


                    BulkCopy.DestinationTableName = "TNSALESSUMMARY2";
                    BulkCopy.WriteToServer(oSource);

                }
            }

            return true;
        }

        public bool SQLBulkCopySniderMaddenCoCustomer(DataTable oSource, string Catalog, string tableName)
        {
            using (SqlConnection connection = TNSQLConnection(Catalog))
            {
                connection.Open();
                //connection.se = 100000;
                using (SqlBulkCopy BulkCopy = new SqlBulkCopy(connection.ConnectionString))
                {
                    BulkCopy.BulkCopyTimeout = 600;
                    BulkCopy.ColumnMappings.Add("CUSTOMERNUMBER", "CUSTOMERNUMBER");
                    BulkCopy.ColumnMappings.Add("CUSTOMERENTERPRISE", "CUSTOMERENTERPRISE");
                    BulkCopy.ColumnMappings.Add("CUSTOMERNAME", "CUSTOMERNAME");
                    BulkCopy.ColumnMappings.Add("CUSTOMERDBA", "CUSTOMERDBA");
                    BulkCopy.ColumnMappings.Add("CUSTOMERADDR1", "CUSTOMERADDR1");
                    BulkCopy.ColumnMappings.Add("CUSTOMERADDR2", "CUSTOMERADDR2");
                    BulkCopy.ColumnMappings.Add("CUSTOMERCITYSTATE", "CUSTOMERCITYSTATE");
                    BulkCopy.ColumnMappings.Add("CUSTOMERZIP", "CUSTOMERZIP");
                    BulkCopy.ColumnMappings.Add("CUSTOMERPHONE1", "CUSTOMERPHONE1");
                    BulkCopy.ColumnMappings.Add("CUSTOMERPHONE2", "CUSTOMERPHONE2");
                    BulkCopy.ColumnMappings.Add("CUSTOMERPHONE3", "CUSTOMERPHONE3");
                    BulkCopy.ColumnMappings.Add("CUSTOMERPHONE4", "CUSTOMERPHONE4");
                    BulkCopy.ColumnMappings.Add("CUSTOMERPHONE5", "CUSTOMERPHONE5");
                    BulkCopy.ColumnMappings.Add("CUSTOMERPHONE6", "CUSTOMERPHONE6");
                    BulkCopy.ColumnMappings.Add("CUSTOMERCELLPHONE", "CUSTOMERCELLPHONE");
                    BulkCopy.ColumnMappings.Add("ACCOUNTOPENED", "ACCOUNTOPENED");
                    BulkCopy.ColumnMappings.Add("STATUS", "STATUS");
                    BulkCopy.ColumnMappings.Add("CREDITLIMIT", "CREDITLIMIT");
                    BulkCopy.ColumnMappings.Add("CUSTOMERHIGHBALANCE", "CUSTOMERHIGHBALANCE");
                    BulkCopy.ColumnMappings.Add("CUSTOMERCLASS", "CUSTOMERCLASS");
                    BulkCopy.ColumnMappings.Add("CUSTOMERLASTACTIVITY", "CUSTOMERLASTACTIVITY");
                    BulkCopy.ColumnMappings.Add("CUSTOMERLASTSTATEMENT", "CUSTOMERLASTSTATEMENT");
                    BulkCopy.ColumnMappings.Add("CUSTOMERLASTPAYMENT", "CUSTOMERLASTPAYMENT");
                    BulkCopy.ColumnMappings.Add("POREQUIRED", "POREQUIRED");
                    BulkCopy.ColumnMappings.Add("VEHICLEREQUIRED", "VEHICLEREQUIRED");
                    BulkCopy.ColumnMappings.Add("SHOPSUPPLYEXEMPT", "SHOPSUPPLYEXEMPT");
                    BulkCopy.ColumnMappings.Add("RETREADCUSTOMER", "RETREADCUSTOMER");
                    BulkCopy.ColumnMappings.Add("TAXSTATE", "TAXSTATE");
                    BulkCopy.ColumnMappings.Add("TAXAUTHORITY", "TAXAUTHORITY");
                    BulkCopy.ColumnMappings.Add("TAXEXEMPTCODE", "TAXEXEMPTCODE");
                    BulkCopy.ColumnMappings.Add("TAXCERTIFICATE", "TAXCERTIFICATE");
                    BulkCopy.ColumnMappings.Add("TERMSCODE", "TERMSCODE");
                    BulkCopy.ColumnMappings.Add("TERMSDECRIPTION", "TERMSDECRIPTION");
                    BulkCopy.ColumnMappings.Add("ROUTECODE", "ROUTECODE");
                    BulkCopy.ColumnMappings.Add("SERVICINGSTORE", "SERVICINGSTORE");
                    BulkCopy.ColumnMappings.Add("ACCOUNTSSALESNUM", "ACCOUNTSSALESNUM");
                    BulkCopy.ColumnMappings.Add("ACCOUNTSALESNAME", "ACCOUNTSALESNAME");
                    BulkCopy.ColumnMappings.Add("CLIENTID", "CLIENTID");

                    BulkCopy.DestinationTableName = tableName;
                    BulkCopy.WriteToServer(oSource);

                }
            }

            return true;
        }

        public bool SQLBulkCopySniderMaddenCoCustomerAssignment(DataTable oSource, string Catalog, string tableName)
        {
            using (SqlConnection connection = TNSQLConnection(Catalog))
            {
                connection.Open();
                //connection.se = 100000;
                using (SqlBulkCopy BulkCopy = new SqlBulkCopy(connection.ConnectionString))
                {
                    BulkCopy.BulkCopyTimeout = 600;
                    BulkCopy.ColumnMappings.Add("CLIENTID", "CLIENTID");
                    BulkCopy.ColumnMappings.Add("CUSTOMERNUMBER", "CUSTOMERNUMBER");
                    BulkCopy.ColumnMappings.Add("SELLINGSALESNUM", "SELLINGSALESNUM");
                    BulkCopy.ColumnMappings.Add("CUSTOMERSALESNUM", "CUSTOMERSALESNUM");


                    BulkCopy.DestinationTableName = tableName;
                    BulkCopy.WriteToServer(oSource);

                }
            }

            return true;
        }




        public bool SQLBulkInsertMaddenCoSalesData(DataTable oSource, string Catalog, string tableName)
        {
            foreach (DataColumn col in oSource.Columns)
            {
                Console.WriteLine(col.ColumnName);
            }
            using (SqlConnection connection = TNSQLConnection(Catalog))
            {
                connection.Open();

                using (SqlBulkCopy BulkCopy = new SqlBulkCopy(connection.ConnectionString))
                {
                    BulkCopy.BulkCopyTimeout = 600;

                    BulkCopy.ColumnMappings.Add("CUSTOMERNUMBER", "CustomerNumber");
                    BulkCopy.ColumnMappings.Add("CUSTOMERCLASSCODE", "CustomerClassCode");
                    BulkCopy.ColumnMappings.Add("CUSTOMERCLASSDESC", "CustomerClassDesc");
                    BulkCopy.ColumnMappings.Add("SALESSTATECODE", "SalesStateCode");
                    BulkCopy.ColumnMappings.Add("STATEPROVINCE", "StateProvince");
                    BulkCopy.ColumnMappings.Add("TAXINGAUTHORITY", "TaxingAuthority");
                    BulkCopy.ColumnMappings.Add("TAXEXEMPTION", "TaxExemption");
                    BulkCopy.ColumnMappings.Add("SALESZIP", "SalesZip");
                    BulkCopy.ColumnMappings.Add("SALESYEARPERIOD", "SalesYearPeriod");
                    BulkCopy.ColumnMappings.Add("SALEPAYMETHOD", "SalePayMethod");
                    BulkCopy.ColumnMappings.Add("SOURCECODE", "SourceCode");
                    BulkCopy.ColumnMappings.Add("SALESSTORE", "SalesStore");
                    BulkCopy.ColumnMappings.Add("CUSTOMERSALESNUM", "CustomerSalesNum");
                    BulkCopy.ColumnMappings.Add("CUSTOMERSALESNAME", "CustomerSalesName");
                    BulkCopy.ColumnMappings.Add("SELLINGSALESNUM", "SellingSalesNum");
                    BulkCopy.ColumnMappings.Add("SELLINGSALESNAME", "SellingSalesName");
                    BulkCopy.ColumnMappings.Add("SALESTYPE", "SalesType");
                    BulkCopy.ColumnMappings.Add("PRODUCTCODE", "ProductCode");
                    BulkCopy.ColumnMappings.Add("PRODUCTLOCCODE", "ProductLocCode");
                    BulkCopy.ColumnMappings.Add("PRODUCTDESCRIPTION", "ProductDescription");
                    BulkCopy.ColumnMappings.Add("PRODUCTCLASS", "ProductClass");
                    BulkCopy.ColumnMappings.Add("PRODUCTCLASSDESC", "ProductClassDesc");
                    BulkCopy.ColumnMappings.Add("PRODUCTBRAND", "ProductBrand");
                    BulkCopy.ColumnMappings.Add("PRODUCTBRANDDESC", "ProductBrandDesc");
                    BulkCopy.ColumnMappings.Add("PRODUCTTYPE", "ProductType");
                    BulkCopy.ColumnMappings.Add("PRODUCTTYPEDESC", "ProductTypeDesc");
                    BulkCopy.ColumnMappings.Add("PRODUCTMFG", "ProductMfg");
                    BulkCopy.ColumnMappings.Add("PRODUCTMFGDESC", "ProductMfgDesc");
                    BulkCopy.ColumnMappings.Add("SALESUNITS", "SalesUnits");
                    BulkCopy.ColumnMappings.Add("SALESPRICELEVEL", "SalesPriceLevel");
                    BulkCopy.ColumnMappings.Add("SALESPRICETTOTAL", "SalesPriceTotal");
                    BulkCopy.ColumnMappings.Add("SALESFETTOTAL", "SalesFetTotal");
                    BulkCopy.ColumnMappings.Add("SALESAVGCOSTTOTAL", "SalesAvgCostTotal");
                    BulkCopy.ColumnMappings.Add("SALESREPCOSTTOTAL", "SalesRepCostTotal");
                    BulkCopy.ColumnMappings.Add("SALESCOMMISSIONCODE", "SalesCommissionCode");
                    BulkCopy.ColumnMappings.Add("SALESTECH", "SalesTech");
                    BulkCopy.ColumnMappings.Add("SALESTECHNAME", "SalesTechName");
                    BulkCopy.ColumnMappings.Add("SALESVEHICLECOUNT", "SalesVehicleCount");
                    BulkCopy.ColumnMappings.Add("SALESSPIFAMOUNT", "SalesSpifAmount");
                    BulkCopy.ColumnMappings.Add("SALESIDENTIFIER", "SalesIdentifier");
                    BulkCopy.ColumnMappings.Add("CUSTOMERTYPE", "CustomerType");
                    BulkCopy.ColumnMappings.Add("CLIENTID", "ClientId");

                    BulkCopy.DestinationTableName = tableName;
                    BulkCopy.WriteToServer(oSource);
                }
            }

            return true;
        }


        public bool SQLBulkCopyLNK(DataTable oSource, string Catalog)
        {
            //   oSource.Columns["Cat"].MaxLength = 50;
            //   oSource.Columns["ProdNum"].MaxLength = 50;


            using (SqlConnection connection = TNSQLConnection(Catalog))
            {
                connection.Open();
                //connection.se = 100000;
                using (SqlBulkCopy BulkCopy = new SqlBulkCopy(connection.ConnectionString))
                {
                    BulkCopy.BulkCopyTimeout = 600;
                    BulkCopy.ColumnMappings.Add("IDT", "InvoiceDate");
                    BulkCopy.ColumnMappings.Add("STR", "STR");
                    BulkCopy.ColumnMappings.Add("LCT", "LCT");
                    BulkCopy.ColumnMappings.Add("TCNDATA", "ProductName");
                    BulkCopy.ColumnMappings.Add("TCNDATA2", "Description");
                    BulkCopy.ColumnMappings.Add("UNITS", "Qty");
                    BulkCopy.ColumnMappings.Add("TOTALSALES", "Price");
                    BulkCopy.ColumnMappings.Add("FEDTAX", "Fet");
                    BulkCopy.ColumnMappings.Add("SALES", "Extension");
                    BulkCopy.ColumnMappings.Add("ISADD", "IsAdd");
                    BulkCopy.ColumnMappings.Add("REPCOST", "Cost");
                    BulkCopy.ColumnMappings.Add("PROFIT", "Profit");
                    BulkCopy.ColumnMappings.Add("TSSMS", "SalesPersonID");
                    BulkCopy.ColumnMappings.Add("TSSMC", "tssmc");
                    BulkCopy.ColumnMappings.Add("PRODUCTCODE", "ProductID");
                    BulkCopy.ColumnMappings.Add("CUSTOMERID", "CustomerID");
                    BulkCopy.ColumnMappings.Add("CLIENTID", "clientid");
                    BulkCopy.ColumnMappings.Add("TSYP2", "MDINVDT");
                    BulkCopy.ColumnMappings.Add("INDSALES", "IndSales");
                    BulkCopy.ColumnMappings.Add("MECHSALES", "MechSales");
                    BulkCopy.ColumnMappings.Add("TSST", "TSST");
                    BulkCopy.ColumnMappings.Add("TSCC", "TSCC");
                    BulkCopy.ColumnMappings.Add("CUUSFLAG6", "CUUSFLAG6");
                    BulkCopy.ColumnMappings.Add("CUNAME", "customerName");
                    BulkCopy.ColumnMappings.Add("TCNKEY", "salespersonKey");
                    BulkCopy.ColumnMappings.Add("dsaid", "dsaid");
                    BulkCopy.ColumnMappings.Add("parentid", "parentid");
                    BulkCopy.ColumnMappings.Add("parentname", "parentname");
                    BulkCopy.ColumnMappings.Add("TSCOSACT", "costAct");
                    BulkCopy.ColumnMappings.Add("Cat", "category");
                    BulkCopy.ColumnMappings.Add("ProdNum", "sku");
                    BulkCopy.ColumnMappings.Add("Manufacturer", "Manufacturer");
                    BulkCopy.ColumnMappings.Add("MFG", "MFG");
                    BulkCopy.ColumnMappings.Add("CUSalesman", "cuSalesperson");
                    BulkCopy.ColumnMappings.Add("CUSalespersonName", "cuSalespersonName");
                    BulkCopy.ColumnMappings.Add("custcity", "custcity");
                    BulkCopy.ColumnMappings.Add("custaddress", "custaddress");



                    BulkCopy.DestinationTableName = "TNSALESSUMMARY2";
                    BulkCopy.WriteToServer(oSource);

                }
            }

            return true;
        }

        public void SQLBulkInsertOrdersToDatabase(DataTable ordersTable)
        {
            using (var connection = TNSQLConnection("Telenotes"))
            {
                connection.Open();
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = "[Integrations].[ExcedeOrders]";

                    bulkCopy.ColumnMappings.Add("tnClientId", "tnClientId");
                    bulkCopy.ColumnMappings.Add("CustomerId", "CustomerId");
                    bulkCopy.ColumnMappings.Add("OrderId", "OrderId");
                    bulkCopy.ColumnMappings.Add("ContractTypeID", "ContractTypeID");
                    bulkCopy.ColumnMappings.Add("ServiceWriter", "ServiceWriter");
                    bulkCopy.ColumnMappings.Add("OrderStatus", "OrderStatus");
                    bulkCopy.ColumnMappings.Add("OrderStatusReadable", "OrderStatusReadable");
                    bulkCopy.ColumnMappings.Add("UnitId", "UnitId");
                    bulkCopy.ColumnMappings.Add("FleetId", "FleetId");
                    bulkCopy.ColumnMappings.Add("CreatedDate", "CreatedDate");
                    bulkCopy.ColumnMappings.Add("OrderType", "OrderType");
                    bulkCopy.ColumnMappings.Add("OrderTotal", "OrderTotal");
                    bulkCopy.ColumnMappings.Add("isVoided", "isVoided");
                    bulkCopy.ColumnMappings.Add("InvoiceNumber", "InvoiceNumber");
                    bulkCopy.ColumnMappings.Add("OrderNumber", "OrderNumber");
                    bulkCopy.ColumnMappings.Add("SalesPerson", "SalesPerson");
                    bulkCopy.ColumnMappings.Add("InvoiceType", "InvoiceType");
                    bulkCopy.ColumnMappings.Add("OrderCost", "OrderCost");
                    bulkCopy.ColumnMappings.Add("EmpId", "EmpId");
                    bulkCopy.ColumnMappings.Add("posted", "posted");
                    bulkCopy.ColumnMappings.Add("BrnId", "BrnId");
                    bulkCopy.ColumnMappings.Add("estimate", "estimate");
                    bulkCopy.ColumnMappings.Add("dateInvoice", "dateInvoice");
                    bulkCopy.ColumnMappings.Add("LaborTotal", "LaborTotal");
                    bulkCopy.ColumnMappings.Add("PartsTotal", "PartsTotal");
                    bulkCopy.ColumnMappings.Add("LaborCostTotal", "LaborCostTotal");
                    bulkCopy.ColumnMappings.Add("PartsCostTotal", "PartsCostTotal");
                    bulkCopy.ColumnMappings.Add("BillingCustomerID", "BillingCustomerID");
                    bulkCopy.ColumnMappings.Add("BillingCustID", "BillingCustID");
                    bulkCopy.ColumnMappings.Add("branch", "branch");

                    bulkCopy.BulkCopyTimeout = 0; // Infinite timeout for large batches

                    bulkCopy.WriteToServer(ordersTable);
                }
            }
        }

        public void SQLBulkInsertOrderPartsToDatabase(DataTable partsTable)
        {
            using (var connection = TNSQLConnection("Telenotes"))
            {
                connection.Open();
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = "[Integrations].[ExcedeOrderParts]";

                    bulkCopy.ColumnMappings.Add("tnClientId", "tnClientId");
                    bulkCopy.ColumnMappings.Add("PartOrderItemID", "PartOrderItemID");
                    bulkCopy.ColumnMappings.Add("OrderId", "OrderId");
                    bulkCopy.ColumnMappings.Add("PartId", "PartId");
                    bulkCopy.ColumnMappings.Add("PartDescription", "PartDescription");
                    bulkCopy.ColumnMappings.Add("QuantityShip", "QuantityShip");
                    bulkCopy.ColumnMappings.Add("QuantityBackOrder", "QuantityBackOrder");
                    bulkCopy.ColumnMappings.Add("CreatedDate", "CreatedDate");
                    bulkCopy.ColumnMappings.Add("OrderStatus", "OrderStatus");
                    bulkCopy.ColumnMappings.Add("Supplier", "Supplier");
                    bulkCopy.ColumnMappings.Add("AmtPrice", "AmtPrice");
                    bulkCopy.ColumnMappings.Add("AmtCost", "AmtCost");
                    bulkCopy.ColumnMappings.Add("SrcId", "SrcId");
                    bulkCopy.ColumnMappings.Add("AmtTax1", "AmtTax1");
                    bulkCopy.ColumnMappings.Add("AmtTax2", "AmtTax2");
                    bulkCopy.ColumnMappings.Add("VIN", "VIN");
                    bulkCopy.ColumnMappings.Add("Year", "Year");
                    bulkCopy.ColumnMappings.Add("Manufacturer", "Manufacturer");
                    bulkCopy.ColumnMappings.Add("itmTyp", "itmTyp");
                    bulkCopy.ColumnMappings.Add("ItemTypeDesc", "ItemTypeDesc");
                    bulkCopy.ColumnMappings.Add("PartsActionType", "PartsActionType");
                    bulkCopy.ColumnMappings.Add("PartType", "PartType");
                    bulkCopy.ColumnMappings.Add("Quantity", "Quantity");

                    bulkCopy.BulkCopyTimeout = 0; // Infinite timeout for large batches

                    bulkCopy.WriteToServer(partsTable);
                }
            }
        }


        public DataSet SQLPT(DataSet oDS, string DSName, string SQLQuery, SqlParameter[] oParam, string Catalog, bool isProc, SqlConnection connection)
        {
            SqlDataAdapter dataAdapter = new SqlDataAdapter(SQLQuery, connection);
            if (isProc)
            {
                dataAdapter.SelectCommand.CommandType = CommandType.StoredProcedure;
            }
            if (oParam != null)
            {
                foreach (SqlParameter oP in oParam)
                {
                    dataAdapter.SelectCommand.Parameters.Add(oP);
                }
            }
            dataAdapter.SelectCommand.CommandTimeout = 0;


            DataSet dataSet = new DataSet();
            dataAdapter.FillSchema(dataSet, SchemaType.Mapped);
            dataAdapter.Fill(dataSet, DSName);

            return dataSet;
        }


        public DataSet SQLPT(DataSet oDS, string DSName, string SQLQuery, SqlParameter[] oParam, string Catalog, bool isProc)
        {

            if (oDS.Tables.Contains(DSName))
            {
                oDS.Tables[DSName].Clear();
                oDS.Tables[DSName].Rows.Clear();
                oDS.Tables[DSName].Dispose();
                oDS.Tables.Remove(DSName);
            }

            using (SqlConnection connection = TNSQLConnection(Catalog))
            {
                SqlDataAdapter dataAdapter = new SqlDataAdapter(SQLQuery, connection);
                if (isProc)
                {
                    dataAdapter.SelectCommand.CommandType = CommandType.StoredProcedure;
                }
                if (oParam != null)
                {
                    foreach (SqlParameter oP in oParam)
                    {
                        dataAdapter.SelectCommand.Parameters.Add(oP);
                    }
                }
                dataAdapter.SelectCommand.CommandTimeout = 500;

                connection.Open();

                DataSet dataSet = new DataSet();
                dataAdapter.Fill(dataSet, DSName);

                return dataSet;
            }
        }
        public DataSet SQLPTODBC(DataSet oDS, string DSName, string SQLQuery, int clientId)
        {

            if (oDS.Tables.Contains(DSName))
            {
                oDS.Tables[DSName].Clear();
                oDS.Tables[DSName].Rows.Clear();
                oDS.Tables[DSName].Dispose();
                oDS.Tables.Remove(DSName);
            }

            using (System.Data.Odbc.OdbcConnection connection = TNODBCConnection(clientId))
            {
                OdbcDataAdapter dataAdapter = new OdbcDataAdapter(SQLQuery, connection);


                dataAdapter.SelectCommand.CommandTimeout = 5000;

                connection.Open();

                DataSet dataSet = new DataSet();
                dataAdapter.Fill(dataSet, DSName);

                return dataSet;
            }
        }

        public DataSet SQLPTPostgres(DataSet oDS, string DSName, string SQLQuery)
        {
            // Clean up existing table if it exists
            if (oDS.Tables.Contains(DSName))
            {
                oDS.Tables[DSName].Clear();
                oDS.Tables[DSName].Rows.Clear();
                oDS.Tables[DSName].Dispose();
                oDS.Tables.Remove(DSName);
            }

            using (NpgsqlConnection connection = TNPostgresConnection()) // <-- Assuming you have a Postgres connection helper
            {
                using (NpgsqlDataAdapter dataAdapter = new NpgsqlDataAdapter(SQLQuery, connection))
                {
                    dataAdapter.SelectCommand.CommandTimeout = 5000;

                    connection.Open();

                    DataSet dataSet = new DataSet();
                    dataAdapter.Fill(dataSet, DSName);

                    return dataSet;
                }
            }
        }


    }
}
