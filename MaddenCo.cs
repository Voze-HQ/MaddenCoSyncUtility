using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using Elasticsearch.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using System.IO;
using System.Configuration;

namespace MaddenCoSyncUtility
{
    class MaddenCo
    {
        public bool GetMaddenCoSalesData(int clientId)
        {
            string insertTable = "integrations.maddencosalesdata";
            string dbName = getDBName(clientId);
            DataSet oDS = new DataSet();

            SQLConnect oC = new SQLConnect();
            int thisYear = DateTime.Now.Year;
            int lastYear = thisYear - 1;

            for (int year = lastYear; year <= thisYear; year++)
            {
                for (int month = 1; month <= 12; month++)
                {
                    string yearMonth = year.ToString() + month.ToString("D2"); // Format as yyyyMM (e.g., 202401)

                    string cODBCSQL = "SELECT DISTINCT " + clientId.ToString() + " AS CLIENTID, " +
                                      "CUSTOMERNUMBER, CUSTOMERCLASSCODE, CUSTOMERCLASSDESC, SALESSTATECODE, STATEPROVINCE, TAXINGAUTHORITY, TAXEXEMPTION, SALESZIP, SALESYEARPERIOD, SALEPAYMETHOD, " +
                                      "SOURCECODE, SALESSTORE, CUSTOMERSALESNUM, CUSTOMERSALESNAME, SELLINGSALESNUM, SELLINGSALESNAME, SALESTYPE, PRODUCTCODE, PRODUCTLOCCODE, PRODUCTDESCRIPTION, " +
                                      "PRODUCTCLASS, PRODUCTCLASSDESC, PRODUCTBRAND, PRODUCTBRANDDESC, PRODUCTTYPE, PRODUCTTYPEDESC, PRODUCTMFG, PRODUCTMFGDESC, SALESUNITS, SALESPRICELEVEL, " +
                                      "SALESPRICETTOTAL, SALESFETTOTAL, SALESAVGCOSTTOTAL, SALESREPCOSTTOTAL, SALESCOMMISSIONCODE, SALESTECH, SALESTECHNAME, SALESVEHICLECOUNT, SALESSPIFAMOUNT, " +
                                      "SALESIDENTIFIER, CUSTOMERTYPE " +
                                      "FROM " + dbName + ".SALESDATA " +
                                      "WHERE " +
                                        //TEST ONLY ??CUSTOMERNUMBER = 8888461 AND " +
                                      "SALESYEARPERIOD = '" + yearMonth + "'";

                    // Optional: Logging
                    Console.WriteLine("Executing query for period: " + yearMonth);


                    // Pull and append to the dataset
                    oDS = oC.SQLPTODBC(oDS, "tSel", cODBCSQL, clientId);
                    oDS.WriteXmlSchema(@"C:\Temp\bauerMaddenCoShape.xml");
                    bulkInsertMaddenCoSalesData(oDS.Tables["tSel"], insertTable, clientId, yearMonth);
                }
            }

            return true;
        }

        public string getDBName(int clientId)
        {
            string dbName = "";
            switch (clientId)
            {
                case 385:
                    dbName = "MCODB539";
                    break;
                case 417:
                    dbName = "MCODB220";
                    break;
                case 1334:
                    dbName = "MCODB559";
                    break;
                case 1366:
                    dbName = "MCODB695";
                    break;
                case 1387:
                    dbName = "MCODB703";
                    break;
                case 1217:
                    dbName = "MCODB257";
                    break;
            }
            return dbName;
        }

        public void syncMaddenCoDirectCustomers()
        {
            DataSet oDS = new DataSet();
            SQLConnect oC = new SQLConnect();
            string sql_client_ids = "select id from tnclients where active = 1 and integrationid = 1 and id != 385";
            string tblName = "getSyncCompanies";
            
            oDS = oC.SQLPT(oDS, tblName, sql_client_ids, null, "Telenotes", false);

            foreach(DataRow oR in oDS.Tables[tblName].Rows)
            {
                this.GetMaddenCoCustomerList((int)oR["id"]);
            }

        }
        public void GetMaddenCoCustomerList(int clientId)
        {
            //Let's update this to use a shared table.
            string dbName = getDBName(clientId);
            SQLConnect oC = new SQLConnect();
            //string dbName = "MCODB220";
            string insertTable = "integrations.maddencocustomers"; 
           
            DataSet oDS = new DataSet();
            

            //string cODBCSQL = "SELECT '" +clientId.ToString() + "' as CLIENTID, * FROM "+ dbName +".CUSTOMERS";
            string cODBCSQL = "select " +clientId.ToString()+ " AS ClientId, CUSTOMERNUMBER, CUSTOMERENTERPRISE, CUSTOMERNAME, CUSTOMERDBA, CUSTOMERADDR1, CUSTOMERADDR2, CUSTOMERCITYSTATE, CUSTOMERZIP, CUSTOMERPHONE1, ";
            cODBCSQL = cODBCSQL + "CUSTOMERPHONE2, CUSTOMERPHONE3, CUSTOMERPHONE4, CUSTOMERPHONE5, CUSTOMERPHONE6, CUSTOMERCELLPHONE, ACCOUNTOPENED, STATUS, CREDITLIMIT, CUSTOMERHIGHBALANCE, ";
            cODBCSQL = cODBCSQL + "CUSTOMERCLASS, CUSTOMERLASTACTIVITY, CUSTOMERLASTSTATEMENT, CUSTOMERLASTPAYMENT, POREQUIRED, VEHICLEREQUIRED, SHOPSUPPLYEXEMPT, RETREADCUSTOMER, TAXSTATE, ";
            cODBCSQL = cODBCSQL + "TAXAUTHORITY, TAXEXEMPTCODE, TAXCERTIFICATE, TERMSCODE, TERMSDECRIPTION, ROUTECODE, SERVICINGSTORE, ACCOUNTSSALESNUM, ACCOUNTSALESNAME ";
            cODBCSQL = cODBCSQL + " FROM " + dbName + ".CUSTOMERS";
            try
            {
                oDS = oC.SQLPTODBC(oDS, "tSel", cODBCSQL, clientId);
                //oDS.Tables["tSel"].Select();
                if (oDS.Tables["tSel"].Rows.Count> 0)
                {
                    bulkInsertMaddenCoCustomer(oDS.Tables["tSel"], insertTable, clientId);
                    string syncCompanies = "exec Integrations.import_maddenCo_Companies " + clientId.ToString();
                    
                    oC.SQLPT(oDS, "sync_companies", syncCompanies, null, "Telenotes", false);
                }

                //ConvertToCsv(oDS.Tables["tSel"], @"c:\temp\SniderMaddenCoCustomers.csv");
            }
            catch(Exception oX)
            {
                Console.Write(oX.ToString());
            }
            
        }

        public bool bulkInsertMaddenCoSalesData(DataTable sqlData, string insertTable, int clientId, string salesYearPeriod)
        {
            SQLConnect sqlConnect = new SQLConnect();
            string sqlDeleteQuery = "delete from " + insertTable + " where clientid = " + clientId.ToString() + " and SalesYearPeriod = '" + salesYearPeriod + "'";
            DataSet oDS = new DataSet();
            sqlConnect.SQLPT(oDS, "delete", sqlDeleteQuery, null, "Telenotes", false);

            sqlConnect.SQLBulkInsertMaddenCoSalesData(sqlData, "Telenotes", insertTable);
            return true;
        }
        public void bulkInsertMaddenCoCustomer(DataTable sqlData,string insertTable, int clientId)
        {
            SQLConnect sqlConnect = new SQLConnect();
            string sqlDeleteQuery = "delete from " + insertTable + " where clientid = " + clientId.ToString();
            DataSet oDS = new DataSet();
            sqlConnect.SQLPT(oDS, "delete", sqlDeleteQuery, null, "Telenotes", false);
            //, oC.TNSQLConnection("Telenotes"));
                  

            sqlConnect.SQLBulkCopySniderMaddenCoCustomer(sqlData, "Telenotes", insertTable);
        }
        public void bulkInsertMaddenCoCustomerAssignments(DataTable sqlData, string insertTable, int clientId)
        {
            SQLConnect sqlConnect = new SQLConnect();
            string sqlDeleteQuery = "delete from " + insertTable + " where clientid = " + clientId.ToString();
            DataSet oDS = new DataSet();
            sqlConnect.SQLPT(oDS, "delete", sqlDeleteQuery, null, "Telenotes", false);


            sqlConnect.SQLBulkCopySniderMaddenCoCustomerAssignment(sqlData, "Telenotes", insertTable);
        }
        public static void ConvertToCsv(DataTable dataTable, string filePath, string delimiter = ",")
        {
            if (dataTable == null)
                throw new ArgumentNullException(nameof(dataTable), "DataTable cannot be null.");

            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));

            StringBuilder csvContent = new StringBuilder();

            // Add header row
            for (int i = 0; i < dataTable.Columns.Count; i++)
            {
                csvContent.Append(dataTable.Columns[i].ColumnName);
                if (i < dataTable.Columns.Count - 1)
                    csvContent.Append(delimiter);
            }
            csvContent.AppendLine();

            // Add data rows
            foreach (DataRow row in dataTable.Rows)
            {
                for (int i = 0; i < dataTable.Columns.Count; i++)
                {
                    string cellValue = row[i]?.ToString();

                    // Escape special characters like the delimiter or newline
                    if (cellValue?.Contains(delimiter) == true || cellValue?.Contains("\n") == true || cellValue?.Contains("\"") == true)
                    {
                        cellValue = "\"" + cellValue.Replace("\"", "\"\"") + "\"";
                    }

                    csvContent.Append(cellValue);
                    if (i < dataTable.Columns.Count - 1)
                        csvContent.Append(delimiter);
                }
                csvContent.AppendLine();
            }

            // Write to file            
            File.WriteAllText(filePath, csvContent.ToString(), Encoding.UTF8);
        }

        public void GetMaddenCoData(int clientId)
        {

            DateTime oNow = DateTime.Now;

            int nMonth = oNow.Month;
            DateTime nMonthPrev = oNow;

            //nMonthPrev = new DateTime(2017,1, 3);
            oNow = new DateTime(2023, 3, 8);
            nMonthPrev = oNow;

            if (oNow.Day == 2 || oNow.Day == 1 || oNow.Day == 3 || oNow.Day == 4 || oNow.Day ==12)
            {
                 nMonthPrev = DateTime.Now.AddMonths(-3);
            }

         //   nMonthPrev = DateTime.Now.AddMonths(-2);
            for (DateTime oD = nMonthPrev; oD <= oNow; oD = oD.AddMonths(1).AddMinutes(-1))
            {
                DateTime oDate = DateTime.Parse(oD.Month.ToString() + "/1/" + nMonthPrev.Year.ToString() + " 08:00:00");

                Console.Write(nMonthPrev.ToShortDateString());
                string sMonth = oDate.Month.ToString();

                string dt = "DTA220";
                if (clientId == 1148)
                {
                    dt = "DTA676";
                }

                string cODBCSQL = "SELECT  '" + oDate.ToString() + "' as IDT,'-1' AS STR, - 1 AS LCT, "+dt+".TMSALE.TSCUS, "+dt+".TMSALE.TSPC AS PRODUCTCODE, "+dt+".TMCN.TCNDATA, "+dt+".TMCN.TCNDATA AS TCNDATA2, SUM("+dt+".TMSALE.TSUNITS) AS UNITS, ";

                cODBCSQL += " 1 AS ISADD, SUM("+dt+ ".TMSALE.TSSALES) + SUM(" + dt + ".TMSALE.TSFET) AS TOTALSALES, SUM("+dt+".TMSALE.TSSALES) AS SALES, SUM("+dt+".TMSALE.TSSALES) - SUM("+dt+".TMSALE.TSCOSREP) ";
                cODBCSQL += " AS PROFIT, "+dt+".TMSALE.TSSMS, "+dt+".TMSALE.TSCUS AS CUSTOMERID, SUM("+dt+".TMSALE.TSFET) AS FEDTAX, SUM("+dt+".TMSALE.TSSALES) AS TOTALSALES, SUM("+dt+".TMSALE.TSCOSREP) ";
                cODBCSQL += " AS REPCOST, " + clientId.ToString() + " AS CLIENTID, '" + oDate.Year.ToString() + sMonth.PadLeft(2, '0') + "' AS TSYP2, C1.CUUSFLAG11 AS INDSALES, C2.CUUSFLAG6 AS DSAID, C1.CUUSFLD6 AS MECHSALES, "+dt+".TMSALE.TSST, "+dt+ ".TMSALE.TSCC,"+dt+".TMSALE.TSSMC, C1.CUUSFLAG6, ";
                cODBCSQL += " C1.CUNAME, "+dt+".TMCN.TCNKEY, C1.CUNUMENT AS parentid, C2.CUNAME AS ParentName, Sum("+dt+".TMSALE.TSCOSACT) as TSCOSACT ";
                cODBCSQL += " FROM            "+dt+".TMSALE, "+dt+".TMCUST C1, "+dt+".TMCUST C2, "+dt+".TMCN ";
                cODBCSQL += " WHERE        "+dt+".TMSALE.TSCUS = C1.CUNUMBER AND C1.CUNUMENT = C2.CUNUMBER AND CONCAT('PCLS0', "+dt+".TMSALE.TSPC) = "+dt+".TMCN.TCNKEY AND("+dt+".TMSALE.TSYP = '" + oDate.Year.ToString() + sMonth.PadLeft(2, '0') + "') AND ";
                cODBCSQL += " ("+dt+".TMSALE.TSPC NOT IN('57', '58', '88', '89', '90', '91', '92', '93', '94', '95', '96', '98', '99')) ";
                cODBCSQL += " GROUP BY C1.CUUSFLAG6, "+dt+".TMSALE.TSCUS, "+dt+".TMSALE.TSPC, "+dt+".TMCN.TCNDATA, "+dt+".TMSALE.TSSMS, C1.CUUSFLAG11, C1.CUUSFLD6, "+dt+".TMSALE.TSST, "+dt+".TMSALE.TSCC, ";
                cODBCSQL += " C2.CUUSFLAG6, C1.CUNAME, "+dt+".TMCN.TCNKEY, C1.CUNUMENT, C2.CUNAME, "+dt+ ".TMSALE.TSSMC " ;
                cODBCSQL += " ORDER BY "+dt+".TMSALE.TSSMS, PRODUCTCODE, "+dt+".TMSALE.TSCUS";


                //string cODBCSQL = "SELECT    '" + oDate.ToString() + "' as IDT,'-1' AS STR,-1 AS LCT,  DTA220.TMSALE.TSCUS, DTA220.TMSALE.TSPC AS PRODUCTCODE, DTA220.TMCN.TCNDATA, DTA220.TMCN.TCNDATA AS TCNDATA2, SUM(DTA220.TMSALE.TSUNITS) AS UNITS, 1 AS ISADD, ";
                //cODBCSQL += " SUM(DTA220.TMSALE.TSSALES) + SUM(DTA220.TMSALE.TSFET) AS TOTALSALES,SUM(DTA220.TMSALE.TSSALES) AS SALES, SUM(DTA220.TMSALE.TSSALES) - SUM(DTA220.TMSALE.TSCOSREP) AS PROFIT, ";
                //cODBCSQL += "  DTA220.TMSALE.TSSMS, DTA220.TMSALE.TSCUS AS CUSTOMERID, SUM(DTA220.TMSALE.TSFET) AS FEDTAX, SUM(DTA220.TMSALE.TSSALES) AS TOTALSALES, ";
                //cODBCSQL += " SUM(DTA220.TMSALE.TSCOSREP) AS REPCOST, 417 AS CLIENTID, '" + oDate.Year.ToString() + sMonth.PadLeft(2, '0') + "' AS TSYP2, DTA220.TMCUST.CUUSFLAG11 AS INDSALES, DTA220.TMCUST.CUUSFLD6 as MECHSALES, DTA220.TMSALE.TSST, DTA220.TMSALE.TSCC,DTA220.TMCUST.CUUSFLAG6  ";
                //cODBCSQL += " FROM            DTA220.TMSALE, DTA220.TMCUST, DTA220.TMCN";
                //cODBCSQL += " WHERE        DTA220.TMSALE.TSCUS = DTA220.TMCUST.CUNUMBER AND CONCAT('PCLS0', DTA220.TMSALE.TSPC) = DTA220.TMCN.TCNKEY AND ";
                //cODBCSQL += " (DTA220.TMSALE.TSYP = '" + oDate.Year.ToString() + sMonth.PadLeft(2, '0') + "') AND (DTA220.TMSALE.TSPC NOT IN ('57', '58', '88', '89', '90', '91','92', '93', '94', '95', '96', '98', '99'))";//92
                //cODBCSQL += " GROUP BY DTA220.TMSALE.TSCUS, DTA220.TMSALE.TSPC, DTA220.TMCN.TCNDATA, DTA220.TMSALE.TSSMS, DTA220.TMCUST.CUUSFLAG11, DTA220.TMCUST.CUUSFLD6, DTA220.TMSALE.TSST, DTA220.TMSALE.TSCC, DTA220.TMCUST.CUUSFLAG6 ";
                //cODBCSQL += " ORDER BY DTA220.TMSALE.TSSMS, PRODUCTCODE, DTA220.TMSALE.TSCUS";

                //string cODBCSQL = "SELECT    '" + oDate.ToString() + "' as IDT,'-1' AS STR,-1 AS LCT,  DTA220.TMSALE.TSCUS, DTA220.TMSALE.TSPC AS PRODUCTCODE, DTA220.TMCN.TCNDATA, DTA220.TMCN.TCNDATA AS TCNDATA2, DTA220.TMSALE.TSUNITS AS UNITS, 1 AS ISADD, ";
                //cODBCSQL += " DTA220.TMSALE.TSSALES + DTA220.TMSALE.TSFET AS TOTALSALES,DTA220.TMSALE.TSSALES AS SALES, DTA220.TMSALE.TSSALES - DTA220.TMSALE.TSCOSREP AS PROFIT, ";
                //cODBCSQL += "  DTA220.TMSALE.TSSMS, DTA220.TMSALE.TSCUS AS CUSTOMERID, DTA220.TMSALE.TSFET AS FEDTAX, DTA220.TMSALE.TSSALES AS TOTALSALES, ";
                //cODBCSQL += " DTA220.TMSALE.TSCOSREP AS REPCOST, 417 AS CLIENTID, '" + oDate.Year.ToString() + sMonth.PadLeft(2, '0') + "' AS TSYP2, DTA220.TMCUST.CUUSFLAG11 AS INDSALES, DTA220.TMCUST.CUUSFLAG6 as DSAID, DTA220.TMCUST.CUUSFLD6 as MECHSALES, DTA220.TMSALE.TSST, DTA220.TMSALE.TSCC,DTA220.TMCUST.CUUSFLAG6, DTA220.TMCUST.CUNAME, DTA220.TMCN.TCNKEY  ";
                //cODBCSQL += " FROM            DTA220.TMSALE, DTA220.TMCUST, DTA220.TMCN";
                //cODBCSQL += " WHERE        DTA220.TMSALE.TSCUS = DTA220.TMCUST.CUNUMBER AND"; // AND
                ////cODBCSQL += "(CONCAT('PCLS0', DTA220.TMSALE.TSPC) = DTA220.TMCN.TCNKEY OR CONCAT('CSUF', DTA220.TMSALE.TSPC) = DTA220.TMCN.TCNKEY OR CONCAT('CSTXT', DTA220.TMSALE.TSPC) = DTA220.TMCN.TCNKEY) AND ";
                //cODBCSQL += "(CONCAT('PCLS0', DTA220.TMSALE.TSPC) = DTA220.TMCN.TCNKEY) AND ";
                //cODBCSQL += " (DTA220.TMSALE.TSYP = '" + oDate.Year.ToString() + sMonth.PadLeft(2, '0') + "') AND (DTA220.TMSALE.TSPC NOT IN ('57', '58', '88', '89', '90', '91','92', '93', '94', '95', '96', '98', '99'))";//92
                ////cODBCSQL += " GROUP BY DTA220.TMCUST.CUUSFLAG6, DTA220.TMSALE.TSCUS, DTA220.TMSALE.TSPC, DTA220.TMCN.TCNDATA, DTA220.TMSALE.TSSMS, DTA220.TMCUST.CUUSFLAG11, DTA220.TMCUST.CUUSFLD6, DTA220.TMSALE.TSST, DTA220.TMSALE.TSCC, DTA220.TMCUST.CUUSFLAG6, DTA220.TMCUST.CUNAME, DTA220.TMCN.TCNKEY";
                //cODBCSQL += " ORDER BY DTA220.TMSALE.TSSMS, PRODUCTCODE, DTA220.TMSALE.TSCUS";


                SQLConnect oC = new SQLConnect();
                DataSet oDS = new DataSet();
                Console.Write("Executing ODBC");
                Console.Write(cODBCSQL);
                try
                {

                    oDS = oC.SQLPTODBC(oDS, "tSel", cODBCSQL, clientId);
                }
                catch (Exception ex) { Console.WriteLine(ex.ToString()); 
                    //Console.ReadKey(); 
                    return; }
                Console.Write("Successful");
                SQL oSQL = new SQL();

                oSQL.ResetSalesSummary(clientId, oDate.Year.ToString() + sMonth.PadLeft(2, '0'));

                oC.SQLBulkCopy(oDS.Tables["tSel"], "Telenotes");

                string invDate = oDate.Month.ToString().PadLeft(2, '0') + @"/01/" + oDate.Year.ToString() + " 08:00:00";

                Console.Clear();
                Console.WriteLine(invDate);
                Console.WriteLine(oDate.Year.ToString() + sMonth.PadLeft(2, '0'));

                sendMaddenCoToElastic(clientId, invDate, oDate.Year.ToString() + sMonth.PadLeft(2, '0'));
            }

        }
        public void sendTest()
        {
            var settings = new Elasticsearch.Net.ConnectionConfiguration(new Uri("https://79c260ab6d384715bf8b4266bb514f59.us-east-1.aws.found.io:9243"));

            settings.PrettyJson();
            settings.BasicAuthentication("elastic", "BJWP4rmHqP1FO0oKdhCeryuM");

            var client = new Elasticsearch.Net.ElasticLowLevelClient(settings);

            sendCompanyLabelToElastic(385, "419557", "Target", client);
        }
        public void sendCompanylabelsToElastic()
        {
            SQLConnect oC = new SQLConnect();
            DataSet oDS = new DataSet();
            string getUpdates = "exec client.getCompanyLabels 385, 377";

            try
            {
                oDS = oC.SQLPT(oDS, "tSel", getUpdates, null, "telenotes", false, oC.TNSQLConnection("Telenotes"));
            }
            catch (Exception ex) { Console.WriteLine(ex.ToString()); 
                //Console.ReadKey(); 
                return; }

            var settings = new Elasticsearch.Net.ConnectionConfiguration(new Uri("https://79c260ab6d384715bf8b4266bb514f59.us-east-1.aws.found.io:9243"));

            settings.PrettyJson();
            settings.BasicAuthentication("elastic", "BJWP4rmHqP1FO0oKdhCeryuM");

            var client = new Elasticsearch.Net.ElasticLowLevelClient(settings);

            foreach (DataRow r in oDS.Tables["tSel"].Rows)
            {
                sendCompanyLabelToElastic(385, r["ProcedeCustomerId"].ToString().Trim(), r["Labels"].ToString().Trim(), client);
            }

        }

        public void GetMaddenCoData_LinkedSQLServer(int clientid)
        {

            DateTime oNow = DateTime.Now;
            //oNow = new DateTime(2023, 9, 8);
            //int nMonth = oNow.Month;
            DateTime nMonthPrev = oNow;

            //nMonthPrev = new DateTime(oNow.Year, nMonth, 1);
           //oNow = new DateTime(2024, 1, 11);
            nMonthPrev = oNow;

            if (oNow.Day == 2 || oNow.Day == 1 || oNow.Day == 3 || oNow.Day == 6 )
            {
                 nMonthPrev = DateTime.Now.AddMonths(-1);
            }

            nMonthPrev = oNow; //AddMonths(-2);
            //nMonthPrev = DateTime.Now.AddMonths(-1);
            for (DateTime oD = nMonthPrev; oD <= DateTime.Now; oD = oD.AddMonths(1).AddMinutes(-1))
            {
                DateTime oDate = DateTime.Parse(oD.Month.ToString() + "/1/" + oD.Year.ToString() + " 08:00:00");

                Console.Write(oD.ToShortDateString());
                string sMonth = oDate.Month.ToString();
                string clinkedSQL = "exec client.SyncMaddenCo 385, '" + oDate.Year.ToString() + sMonth.PadLeft(2, '0') + "'";


                SQLConnect oC = new SQLConnect();
                SQL oSQL = new SQL();
                DataSet oDS = new DataSet();
                Console.Write("Executing Linked Server");
                Console.Write(clinkedSQL);
                try
                {
                     oDS = oC.SQLPT(oDS, "tSel", clinkedSQL, null, "telenotes", false, oC.TNSQLLinkedConnection("Telenotes"));
                    //DataTable oT = oSQL.GetMaddenCoData(385, oDate.Year.ToString() + sMonth.PadLeft(2, '0'));
                    //oDS.Tables.Add(oT);//TSel set in function call above.
                }
                catch (Exception ex) { Console.WriteLine(ex.ToString()); 
                    //Console.ReadKey(); 
                    return; }

                foreach(DataRow oR in oDS.Tables["tSel"].Rows)
                {
                    if (oR["TSST"].ToString().Trim() == "11" || oR["TSST"].ToString().Trim() == "511")
                    {
                        String tssms = oR["TSSMS"].ToString().ToString();
                        String tssmc = oR["TSSMC"].ToString().ToString();
                        //oR["TSSMS"] = Convert.ToInt32(tssmc);
                        //oR["TSSMC"] = Convert.ToInt32(tssms);
                    }
                }

                Console.Write("Successful");
                //  Console.ReadKey();
                //SQL dtSQL = new SQL();
                Console.Write(oDS.Tables["tSel"].Rows.Count.ToString());
                oSQL.ResetSalesSummary(clientid, oDate.Year.ToString() + sMonth.PadLeft(2, '0'));

                oC.SQLBulkCopyLNK(oDS.Tables["tSel"], "Telenotes");
                string invDate = oDate.Month.ToString().PadLeft(2, '0') + @"/01/" + oDate.Year.ToString() + " 08:00:00";

                sendMaddenCoToElastic(385, invDate, oDate.Year.ToString() + sMonth.PadLeft(2, '0'));

                string s = "";
            }

        }

        public void SendToElastic(int clientid)
        {

            DateTime oNow = DateTime.Now;

            int nMonth = oNow.Month;
            DateTime nMonthPrev = oNow;

            //nMonthPrev = new DateTime(2021, 8, 1);
            //oNow = new DateTime(2016, 8, 8);

            if (oNow.Day == 2 || oNow.Day == 1 || oNow.Day == 3 || oNow.Day == 4)
            {
                // nMonthPrev = DateTime.Now.AddMonths(-13);
            }

            nMonthPrev = DateTime.Now.AddDays(-4);
            for (DateTime oD = nMonthPrev; oD <= oNow; oD = oD.AddMonths(1).AddMinutes(-1))
            {
                DateTime oDate = DateTime.Parse(oD.Month.ToString() + "/1/" + oD.Year.ToString() + " 08:00:00");

                Console.Write(oD.ToShortDateString());
                string sMonth = oDate.Month.ToString();
                string invDate = oDate.Month.ToString().PadLeft(2, '0') + @"/01/" + oDate.Year.ToString() + " 08:00:00";

                sendMaddenCoToElastic(385, invDate, oDate.Year.ToString() + sMonth.PadLeft(2, '0'));

                string s = "";
            }

        }
        public void GetMaddenCoCustomers(int clientId)
        {
            string cODBCSQL = "SELECT CUADDRESS1, CUADDRESS3, CUADDRESS4, CUZIP, CUCONTACT, CUDTECRT, CUCSTID, CUEMAIL, CUEMPLOYER, CUEXPHONE, CUEXPHONE2, CUEXPHONE3, CUNAME, CUNUMBER, CUNUMFAX, CUPHONE, CUSALESMAN FROM DTA676.TMCUST where CUDELETECD = 'A' and CUNUMBER NOT IN (0,9,99,100)";
            SQLConnect oC = new SQLConnect();
            DataSet oDS = new DataSet();
            oDS = oC.SQLPTODBC(oDS, "tSel", cODBCSQL, clientId);
            SQL oSQL = new SQL();
            int nON = 0;

            foreach (DataRow oR in oDS.Tables["tSel"].Rows)
            {
                nON++;
                Console.Title = "On Record: " +  nON.ToString();


                if (oR["CUNAME"].ToString().Trim() != "")
                {
                    int CustID = Convert.ToInt32(oR["CUNUMBER"].ToString());
                    string cuName = oR["CUNAME"].ToString();
                    String address = oR["CUADDRESS1"].ToString().Trim();
                    String address2 = oR["CUADDRESS3"].ToString().Trim();
                    String[] address2Parsed = address2.Split(',');
                    String city = address2;
                    String state = "";
                    if (address2Parsed.Length > 1)
                    {
                        city = address2Parsed[0].Trim();
                        state = address2Parsed[1].Trim();
                    }
                    String zip = oR["CUZIP"].ToString();
                    if (zip.IndexOf('-') > 0)
                    {
                        zip = zip.Substring(0, zip.IndexOf('-'));
                    }
                    String createDate = oR["CUDTECRT"].ToString();
                    String contactFN = oR["CUCONTACT"].ToString().Trim();
                    String contactLN = "";
                    String[] splt = new string[] { " " };
                    String[] contact = contactFN.Split(splt, 2, StringSplitOptions.RemoveEmptyEntries);
                    if (contact.Length > 0)
                    {
                        contactFN = contact[0].Trim();
                        if (contact.Length > 1)
                        {
                            contactLN = contact[1].Trim();
                        }
                    }
                    
                    String phone = "";
                    if (oR["CUEXPHONE"].ToString().Trim() != "0")
                    {
                        phone = oR["CUEXPHONE"].ToString();
                    }
                    else if (oR["CUEXPHONE2"].ToString().Trim() != "0")
                    {

                        phone = oR["CUEXPHONE2"].ToString();

                    } 
                    String contPhone = "";
                    if (oR["CUEXPHONE3"].ToString().Trim() != "0")
                    {
                        contPhone = oR["CUEXPHONE3"].ToString().Trim();
                    }

                    String CustNo = oR["CUNUMBER"].ToString().Trim();
                    String fax = oR["CUNUMFAX"].ToString();
                    String cusPhone = oR["CUPHONE"].ToString();
                    String salesId = oR["CUSALESMAN"].ToString();


                    int nRet = oSQL.AddCustomer(cuName, CustNo, clientId, address, city, state, zip, phone, fax, contactFN, contactLN, cusPhone, salesId);
                }
                //int nRet = oSQL.AddSalesSummary("", oDate, oR["TCNDATA"].ToString(), "-1", -1, oR["TCNDATA"].ToString(), Convert.ToDouble(oR["UNITS"].ToString()), Convert.ToDouble(oR["TOTALSALES"].ToString()), oR["FEDTAX"].ToString(), Convert.ToDouble(oR["SALES"].ToString()), true, Convert.ToDouble(oR["REPCOST"].ToString()), Convert.ToDouble(oR["PROFIT"].ToString()), Convert.ToInt32(oR["TSSMS"].ToString()), oR["PRODUCTCODE"].ToString(), Convert.ToInt32(oR["TSCUS"].ToString()), 417);
            }

        }
        public void GetMaddenCoCustomerRank()
        {

            for (int n = 2; n <= 2; n++)
            {
                DateTime oDate = DateTime.Parse(n.ToString().Trim() + "/1/2015 08:00:00");
                string sMonth = oDate.Month.ToString().Trim().PadLeft(2, '0');




                string cODBCSQL = "SELECT        DTA220.TMSALE.TSCUS, DTA220.TMCUST.CUNAME, SUM(DTA220.TMSALE.TSUNITS) AS UNITS, SUM(DTA220.TMSALE.TSCOSREP) AS REPCOST, ";
                cODBCSQL += "         SUM(DTA220.TMSALE.TSSALES) AS SALES, SUM(DTA220.TMPROD.PDFEDTAX) AS FEDTAX, SUM(DTA220.TMSALE.TSFET) AS FET, ";
                cODBCSQL += " DTA220.TMSALE.TSSMS AS SALESNO";
                cODBCSQL += " FROM            DTA220.TMSALE, DTA220.TMCUST, DTA220.TMPROD";
                cODBCSQL += " WHERE        (DTA220.TMSALE.TSCUS = DTA220.TMCUST.CUNUMBER) AND (DTA220.TMSALE.TSYP = '" + oDate.Year.ToString() + sMonth + "') AND (DTA220.TMSALE.TSPD = DTA220.TMPROD.PDNUMBER) ";
                cODBCSQL += "           AND (DTA220.TMSALE.TSST = DTA220.TMPROD.PDSTORE) AND (DTA220.TMSALE.TSPC NOT IN ('32', '58', '88', '89', '90', '91', ' 92', '93', '94', '95', '96', '98', '99')) ";
                //cODBCSQL += " AND (DTA220.TMSALE.TSSMS = 1571)";
                cODBCSQL += " GROUP BY DTA220.TMCUST.CUNAME, DTA220.TMSALE.TSCUS, DTA220.TMSALE.TSSMS";

                cODBCSQL += " ORDER BY SALES DESC ";


                SQLConnect oC = new SQLConnect();
                DataSet oDS = new DataSet();
                oDS = oC.SQLPTODBC(oDS, "tSel", cODBCSQL, 417);
                SQL oSQL = new SQL();
                foreach (DataRow oR in oDS.Tables["tSel"].Rows)
                {


                    int CustID = Convert.ToInt32(oR["TSCUS"].ToString());
                    string CUName = oR["CUNAME"].ToString();
                    double Units = Convert.ToDouble(oR["Units"].ToString());
                    double RepCost = Convert.ToDouble(oR["REPCOST"].ToString());
                    double Sales = Convert.ToDouble(oR["SALES"].ToString());
                    double FedTax = Convert.ToDouble(oR["FEDTAX"].ToString());
                    double FET = Convert.ToDouble(oR["FET"].ToString());
                    int SalesNo = Convert.ToInt32(oR["SALESNO"].ToString());

                    int nRet = oSQL.AddCustomerRank(CustID, CUName, Units, RepCost, Sales, FedTax, FET, SalesNo, oDate);

                    //int nRet = oSQL.AddSalesSummary("", oDate, oR["TCNDATA"].ToString(), "-1", -1, oR["TCNDATA"].ToString(), Convert.ToDouble(oR["UNITS"].ToString()), Convert.ToDouble(oR["TOTALSALES"].ToString()), oR["FEDTAX"].ToString(), Convert.ToDouble(oR["SALES"].ToString()), true, Convert.ToDouble(oR["REPCOST"].ToString()), Convert.ToDouble(oR["PROFIT"].ToString()), Convert.ToInt32(oR["TSSMS"].ToString()), oR["PRODUCTCODE"].ToString(), Convert.ToInt32(oR["TSCUS"].ToString()), 417);
                }
            }


        }
        public class SalesRecord
        {
            public int id { get; set; }
            public string companyname { get; set; }
            public DateTime invoicedate { get; set; }
            public string productname { get; set; }
            public string description { get; set; }
            public decimal qty { get; set; }
            public decimal price { get; set; }
            public decimal fet { get; set; }
            public decimal extension { get; set; }
            public int isadd { get; set; }
            public decimal cost { get; set; }
            public decimal profit { get; set; }
            public int salespersonid { get; set; }
            public int tssmc { get; set; }
            public string salespersoncategory { get; set; }
            public string productid { get; set; }
            public int customerid { get; set; }
            public string tsst { get; set; }
            public string tscc { get; set; }
            public string cuusflag6 { get; set; }
            public string mdinvdt { get; set; }
            public int agentmailbox { get; set; }
            public int agentid { get; set; }
            public int agencyid { get; set; }
            public string repname { get; set; }
            public int year { get; set; }
            public string salespersonkey { get; set; }
            public string repidentity { get; set; }
            public string dsaid { get; set; }
            public string parentid { get; set; }
            public string parentname { get; set; }
            public decimal costAct { get; set; }
            public string category { get; set; }
            public string sku { get; set; }
            public string Manufactuer { get; set; }
            public string Manufacturer { get; set; }
            public string MFG { get; set; }
            public string brnid { get; set; }
            public int cuSalespersonId { get; set; }
            public string cuSalespersonName { get; set; }
            public string custcity { get; set; }
            public string custaddress { get; set; }
            public string tnCustomerId { get; set; }

        }

        public async Task<StringResponse> IndexElastic(SalesRecord oRec, ElasticLowLevelClient client)
        {
            //client.Index<StringResponse>
            var _return = await client.IndexAsync<StringResponse>("maddencov2", oRec.id.ToString(), PostData.Serializable(oRec), null);

            Console.SetCursorPosition(0, 22);
            Console.WriteLine(_return.ToString());
            return _return;
            //string responseString = asyncIndexResponse.Body;
            //return responseString;
        }
        public async Task<StringResponse> DeleteMonthMaddenCo(String Date, ElasticLowLevelClient client, int agencyId)
        {

            try
            {
                var _return = client.DeleteByQuery<StringResponse>("maddencov2", PostData.Serializable(new
                {
                    query = new
                    {

                        @bool = new
                        {
                            must = new Object[]
                            {
                                new {
                                     match = new
                                     {
                                         mdinvdt = Date
                                     }
                                },
                                new {
                                     match = new
                                     {
                                         agencyid = agencyId
                                     }
                                }


                            }
                            
                        }
                    }
                    //query = new
                    //{
                    //   match  = new
                    //    {
                    //        mdinvdt = Date
                            
                    //    }
                    //   &&
                    //   match = new bool
                    //   {
                    //       agencyid = agencyId
                    //   }
                    //}
                    
                }));


                return _return;
            }
            catch (Exception oEx)
            {
                Console.WriteLine(oEx.ToString());
            }
            return null;
            //string responseString = asyncIndexResponse.Body;
            //return responseString;
        }
        public StringResponse sendCompanyLabelToElastic(int agencyId, string maddenCoId, string Labels, ElasticLowLevelClient client)
        {
            if (maddenCoId == "0" || maddenCoId.ToString().Trim() =="")
            {
                return null;
            }

            int mcId = 0;
            try
            {
                mcId = Convert.ToInt32(maddenCoId);
            }catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
                //Console.ReadKey();
            }
            string[] labels = Labels.Split(',');
            string lbl = "";
            int n = 0;
            foreach(String s in labels)
            {
                if (n > 0)
                {
                    lbl = lbl + ",";
                }
                lbl = lbl + "'" + s + "'";
                n++;
            }

           
            var sscript = $"ctx._source.companytype= [" + lbl + "]";//params.Id;";


            try
            {

            
                var _return = client.UpdateByQuery<StringResponse>("maddencov2", PostData.Serializable(new
                {
                    script = sscript,
           
                    query = new
                    {

                        @bool = new
                        {
                            must = new Object[]
                            {
                                new {
                                     match = new
                                     {
                                         customerid = mcId
                                     }
                                },
                                new {
                                     match = new
                                     {
                                         agencyid = agencyId
                                     }
                                }


                            }

                        }
                    },
                    
                }));

                Console.WriteLine(_return.Body.ToString());
                return _return;
            }
            catch (Exception oEx)
            {
                Console.WriteLine(oEx.ToString());
            }
            return null;

        }
        public void sendMaddenCoToElastic(int clientId, string InvDate, string DelDate)
        {

            String elasticHost = Properties.Settings.Default.Properties["ElasticHost"].DefaultValue.ToString();
            String elasticKey = Properties.Settings.Default.Properties["ElasticKey"].DefaultValue.ToString();
            var settings = new Elasticsearch.Net.ConnectionConfiguration(new Uri(elasticHost));

            settings.BasicAuthentication("elastic", elasticKey);
            
            //settings.BasicAuthentication("elastic", "BJWP4rmHqP1FO0oKdhCeryuM");

            var client = new Elasticsearch.Net.ElasticLowLevelClient(settings);
            Console.WriteLine("deleting " + InvDate);

            DeleteMonthMaddenCo(DelDate, client, clientId);

            SQL oSQL = new SQL();
            Console.WriteLine("Getting Data to Rebuild Month");
            DataTable dt = oSQL.GetSalesSummary(clientId, InvDate);
            Console.WriteLine("Retrieved Data");
            List<SalesRecord> oList = new List<SalesRecord>();

            int nOn = 1;
            Console.WriteLine(InvDate + ": " + dt.Rows.Count.ToString() + " --- ");

            Console.WriteLine("Record Count");
            Console.WriteLine(dt.Rows.Count.ToString());
            List<SalesRecord> oSendBatch = new List<SalesRecord>();
            //Parallel.ForEach(dt.Rows.OfType<System.Data.DataRow>(), oRow =>
            foreach (DataRow oRow in dt.Rows)
            {
                Console.SetCursorPosition(0, 17);
                Console.WriteLine(nOn.ToString());
                try
                {
                    SalesRecord oRec = new SalesRecord();
                    oRec.cost = oRow["cost"] as decimal? ?? 0;
                    oRec.customerid = oRow["Customerid"] as int? ?? 0;
                    oRec.cuusflag6 = oRow["cuusflag6"] as string;
                    oRec.description = oRow["description"] as string;
                    oRec.extension = oRow["extension"] as decimal? ?? 0;
                    oRec.fet = oRow["fet"] as decimal? ?? 0;
                    oRec.costAct = oRow["costAct"] as decimal? ?? 0;
                    oRec.id = oRow["id"] as int? ?? 0;
                    //TimeSpan t = new TimeSpan(((oRow["invoicedate"] as DateTime? ?? DateTime.Parse("1/1/1900 00:00:00"))- new DateTime (1970,1,1)).Ticks);

                    //oRec.invoicedate = (int)t.TotalSeconds;
                    oRec.invoicedate = oRow["invoicedate"] as DateTime? ?? DateTime.Parse("1/1/1900 00:00:00");

                    oRec.year = (oRow["invoicedate"] as DateTime? ?? DateTime.Parse("1/1/1900 00:00:00")).Year;
                    oRec.isadd = oRow["isAdd"] as int? ?? 0;
                    oRec.price = oRow["Price"] as decimal? ?? 0;
                    oRec.productid = oRow["ProductId"] as string;
                    try { 
                        oRec.productname = (oRow["ProductName"] as string).Trim();
                    }
                    catch(Exception ex)
                    {
                        oRec.productname = "";
                    }
                    oRec.profit = oRow["profit"] as decimal? ?? 0;
                    oRec.qty = oRow["qty"] as decimal? ?? 0;
                    oRec.salespersonid = oRow["salespersonid"] as int? ?? 0;
                    oRec.tssmc = oRow["tssmc"] as int? ?? 0;
                    oRec.tscc = oRow["tscc"] as string;
                    oRec.tsst = oRow["tsst"] as string;
                    oRec.mdinvdt = (oRow["MDINVDT"] as string).Trim();
                    oRec.agentid = oRow["agentId"] as int? ?? 0;
                    oRec.agentmailbox = oRow["mailbox"] as int? ?? 0;
                    oRec.agencyid = oRow["clientId"] as int? ?? 0;
                    oRec.repname = oRow["repname"] as string;
                    string brnid = oRow["brnid"] as string;

                    oRec.brnid = brnid.ToString().Trim();
                    
                    oRec.salespersonkey = oRow["salespersonKey"] as string;
                    oRec.companyname = oRow["customerName"] as string;
                    if (oRow["RepID"].ToString() == "")
                    {
                        oRec.repidentity = "";
                    }
                    else
                    {
                        oRec.repidentity = (oRow["RepID"] as string).Trim();
                    }
                    if (oRow["dsaid"].ToString() == "")
                    {
                        oRec.dsaid = "";
                    }
                    else
                    {
                        oRec.dsaid = oRow["dsaid"].ToString().Trim();
                    }
                    if (oRow["parentid"].ToString() == "")
                    {
                        oRec.parentid = "0";
                    }
                    else
                    {
                        oRec.parentid = oRow["parentid"].ToString();
                    }
                    if (oRow["parentname"].ToString() == "")
                    {
                        oRec.parentname = "";
                    }
                    else
                    {
                        oRec.parentname = oRow["parentname"].ToString();
                    }



                    if (oRow["Category"] != null)
                    {
                        oRec.category = oRow["Category"].ToString().Trim();
                    }
                    if (oRow["Manufacturer"] != null)
                    {
                        oRec.Manufacturer = oRow["Manufacturer"].ToString().Trim();
                        oRec.Manufactuer = oRow["Manufacturer"].ToString().Trim();
                    }
                    if (oRow["sku"] != null)
                    {
                        oRec.sku = oRow["sku"].ToString().Trim();
                    }
                    if (oRow["MFG"] != null)
                    {
                        oRec.MFG = oRow["MFG"].ToString().Trim();
                    }
                    if (oRow["cuSalesperson"] != null )
                    {
                        if (oRow["cuSalesperson"].GetType().FullName != "System.DBNull")
                        {
                            oRec.cuSalespersonId = Convert.ToInt32(oRow["cuSalesperson"].ToString().Trim());
                        }
                    }
                    if (oRow["custcity"] != null)
                    {
                        oRec.custcity = oRow["custcity"].ToString().Trim();
                    }
                    if (oRow["custaddress"] != null)
                    {
                        oRec.custaddress = oRow["custaddress"].ToString().Trim();
                    }
                
                
                if (oRow["cuSalespersonName"] != null)
                    {
                        if (oRow["cuSalespersonName"].GetType().FullName != "System.DBNull")
                        {
                            oRec.cuSalespersonName = oRow["cuSalespersonName"].ToString().Trim();
                        }
                    }

                if (oRow["TNID"].GetType().FullName != "System.DBNull")
                    {
                        oRec.tnCustomerId = oRow["TNID"].ToString().Trim();
                    }
                    //var response1 = client.Index<StringResponse>("maddenco", "doc", oRec.id.ToString(), PostData.Serializable(oRec));
                    var response = IndexElastic(oRec, client);
           
                    nOn++;
                }
                catch (Exception oEx)
                {
                    Console.WriteLine(oEx.ToString());
                    Console.WriteLine(oRow["id"].ToString());
                }

            }
    

        }
    }

}
