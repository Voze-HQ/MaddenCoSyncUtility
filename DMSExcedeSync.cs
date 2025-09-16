using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace MaddenCoSyncUtility
{
    public class DMSExcedeSync
    {
        public bool GetExcedeDMSOrdersData(Guid clientId, int numClientId)
        {
            string insertTable = "integrations.ExcedeOrders";

            DataSet oDS = new DataSet();

            SQLConnect oC = new SQLConnect();
            int thisYear = DateTime.Now.Year;
            int lastYear = thisYear - 1;

            for (int year = lastYear; year <= thisYear; year++)
            {
                for (int month = 1; month <= 12; month++)
                {
                    // Skip future months in current year
                    if (year == thisYear && month > DateTime.Now.Month)
                        break;

                    // Construct the first day of the month
                    DateTime startDate = new DateTime(year, month, 1);

                    // Get the first day of the next month
                    DateTime endDate = startDate.AddMonths(1);

                    // Optional: Format for logging or use
                    string yearMonth = year.ToString() + month.ToString("D2");

                    //string cPGSQL = "SELECT * ";
                    //cPGSQL += "FROM excede.ptsls ";
                    //cPGSQL += "WHERE client_id = 'e24d19b6-c1d3-4fac-bb16-dfbc42a3250c' ";
                    //cPGSQL += $"AND datecreate >= DATE '{startDateStr}' ";
                    //cPGSQL += $"AND datecreate < DATE '{endDateStr}';";
                    string ordersQuery = BuildClientOrdersQuery(clientId, startDate, endDate);
                    // Optional: Logging
                    Console.WriteLine("Executing query for period: " + yearMonth);

                    // Pull and append to the dataset
                    oDS = oC.SQLPTPostgres(oDS, "tSel", ordersQuery);
                    Console.WriteLine(oDS.Tables["tSel"].Rows.Count);

                    bulkInsertDMSOrdersData(
                        oDS.Tables["tSel"],
                        "integrations.excedeOrders",
                        numClientId,
                        month,
                        year,
                        3 // 3 == parts order
                    );
                }
            }
            return true;
        }

        public bool GetExcedeDMSOrderPartsData(Guid clientId, int numClientId)
        {
            string insertTable = "integrations.ExcedeOrderParts";

            DataSet oDS = new DataSet();

            SQLConnect oC = new SQLConnect();
            int thisYear = DateTime.Now.Year;
            int lastYear = thisYear - 1;

            for (int year = lastYear; year <= thisYear; year++)
            {
                for (int month = 1; month <= 12; month++)
                {
                    // Skip future months in current year
                    if (year == thisYear && month > DateTime.Now.Month)
                        break;

                    // Construct the first day of the month
                    DateTime startDate = new DateTime(year, month, 1);

                    // Get the first day of the next month
                    DateTime endDate = startDate.AddMonths(1);

                    // Optional: Format for logging or use
                    string yearMonth = year.ToString() + month.ToString("D2");

                    //string cPGSQL = "SELECT * ";
                    //cPGSQL += "FROM excede.ptsls ";
                    //cPGSQL += "WHERE client_id = 'e24d19b6-c1d3-4fac-bb16-dfbc42a3250c' ";
                    //cPGSQL += $"AND datecreate >= DATE '{startDateStr}' ";
                    //cPGSQL += $"AND datecreate < DATE '{endDateStr}';";
                    string ordersQuery = BuildClientOrderPartsQuery(clientId, startDate, endDate);
                    // Optional: Logging
                    Console.WriteLine("Executing query for period: " + yearMonth);

                    // Pull and append to the dataset
                    oDS = oC.SQLPTPostgres(oDS, "tSel", ordersQuery);
                    Console.WriteLine(oDS.Tables["tSel"].Rows.Count);

                    bulkInsertDMSOrderPartsData(
                        oDS.Tables["tSel"],
                        "integrations.excedeOrderParts",
                        numClientId,
                        month,
                        year
                    );
                }
            }

            return true;
        }


        public bool bulkInsertDMSOrdersData(DataTable sqlData, string insertTable, int clientId, int month, int year, int orderType)
        {
            SQLConnect sqlConnect = new SQLConnect();
            string sqlDeleteQuery = $"delete from integrations.excedeorders where tnclientid = {clientId} and month(createddate) = {month} and year(createddate) = {year}";
            DataSet oDS = new DataSet();
            sqlConnect.SQLPT(oDS, "delete", sqlDeleteQuery, null, "Telenotes", false);
            DataTable mappedData = MapOrdersToDataTable(sqlData, clientId);
            Console.Write(mappedData);
            sqlConnect.SQLBulkInsertOrdersToDatabase(mappedData);
            return true;
        }

        public bool bulkInsertDMSOrderPartsData(DataTable sqlData, string insertTable, int clientId, int month, int year)
        {
            SQLConnect sqlConnect = new SQLConnect();
            string sqlDeleteQuery = $"delete from integrations.excedeorderparts where tnclientid = {clientId} and month(createddate) = {month} and year(createddate) = {year}";
            DataSet oDS = new DataSet();
            sqlConnect.SQLPT(oDS, "delete", sqlDeleteQuery, null, "Telenotes", false);
            DataTable mappedData = MapOrdersPartsToDataTable(sqlData, clientId);
            Console.Write(mappedData);
            sqlConnect.SQLBulkInsertOrderPartsToDatabase(mappedData);
            return true;
        }
        private DataTable CreateExcedeOrdersDataTable()
        {
            var table = new DataTable("orderData");

            table.Columns.Add("tnClientId", typeof(int));
            table.Columns.Add("CustomerId", typeof(string));
            table.Columns.Add("OrderId", typeof(string));
            table.Columns.Add("ContractTypeID", typeof(string));
            table.Columns.Add("ServiceWriter", typeof(string));
            table.Columns.Add("OrderStatus", typeof(int));
            table.Columns.Add("OrderStatusReadable", typeof(string));
            table.Columns.Add("UnitId", typeof(string));
            table.Columns.Add("FleetId", typeof(string));
            table.Columns.Add("CreatedDate", typeof(DateTime));
            table.Columns.Add("OrderType", typeof(int));
            table.Columns.Add("OrderTotal", typeof(decimal));
            table.Columns.Add("isVoided", typeof(bool));
            table.Columns.Add("InvoiceNumber", typeof(string));
            table.Columns.Add("OrderNumber", typeof(string));
            table.Columns.Add("SalesPerson", typeof(string));
            table.Columns.Add("InvoiceType", typeof(string));
            table.Columns.Add("OrderCost", typeof(decimal));
            table.Columns.Add("EmpId", typeof(string));
            table.Columns.Add("posted", typeof(int));
            table.Columns.Add("BrnId", typeof(string));
            table.Columns.Add("estimate", typeof(int));
            table.Columns.Add("dateInvoice", typeof(DateTime));
            table.Columns.Add("LaborTotal", typeof(decimal));
            table.Columns.Add("PartsTotal", typeof(decimal));
            table.Columns.Add("LaborCostTotal", typeof(decimal));
            table.Columns.Add("PartsCostTotal", typeof(decimal));
            table.Columns.Add("BillingCustomerID", typeof(int));
            table.Columns.Add("BillingCustID", typeof(string));
            table.Columns.Add("branch", typeof(string));

            return table;
        }
        private DataTable CreateExcedeOrderPartsDataTable()
        {
            var table = new DataTable("partItems");

            table.Columns.Add("tnClientId", typeof(int));
            table.Columns.Add("PartOrderItemID", typeof(string));
            table.Columns.Add("OrderId", typeof(string));
            table.Columns.Add("PartId", typeof(string));
            table.Columns.Add("PartDescription", typeof(string));
            table.Columns.Add("QuantityShip", typeof(long));
            table.Columns.Add("QuantityBackOrder", typeof(long));
            table.Columns.Add("CreatedDate", typeof(DateTime));
            table.Columns.Add("OrderStatus", typeof(string));
            table.Columns.Add("Supplier", typeof(string));
            table.Columns.Add("AmtPrice", typeof(decimal));
            table.Columns.Add("AmtCost", typeof(decimal));
            table.Columns.Add("SrcId", typeof(string));
            table.Columns.Add("AmtTax1", typeof(decimal));
            table.Columns.Add("AmtTax2", typeof(decimal));
            table.Columns.Add("VIN", typeof(string));
            table.Columns.Add("Year", typeof(int));
            table.Columns.Add("Manufacturer", typeof(string));
            table.Columns.Add("itmTyp", typeof(int));
            table.Columns.Add("ItemTypeDesc", typeof(string));
            table.Columns.Add("PartsActionType", typeof(string));
            table.Columns.Add("PartType", typeof(string));
            table.Columns.Add("Quantity", typeof(decimal));

            return table;
        }


        private DataTable MapOrdersToDataTable(DataTable orders, int clientId)
        {
            var table = CreateExcedeOrdersDataTable();

            foreach (DataRow order in orders.Rows)
            {
                var row = table.NewRow();

                row["tnClientId"] = clientId;
                row["CustomerId"] = order["customerid"]?.ToString() ?? (object)DBNull.Value;
                row["BillingCustId"] = order["customerid"]?.ToString() ?? (object)DBNull.Value;  // Assuming bill-to = customerid in new structure
                row["OrderId"] = order["orderid"]?.ToString() ?? (object)DBNull.Value;
                row["ContractTypeID"] = DBNull.Value; // Not available
                row["ServiceWriter"] = order["servicewriter"] ?? (object)DBNull.Value;
                row["OrderStatus"] = order["status"] ?? (object)DBNull.Value;

                string orderStatusReadable = "";
                if (order["status"] != DBNull.Value)
                {
                    int status = Convert.ToInt32(order["status"]);
                    switch (status)
                    {
                        case 644:
                            orderStatusReadable = "Open";
                            break;
                        case 645:
                        case 753:
                            orderStatusReadable = "Invoiced";
                            break;
                        default:
                            orderStatusReadable = "";
                            break;
                    }
                }
                row["OrderStatusReadable"] = orderStatusReadable;
                row["CustomerId"] = order["customerid"] ?? (object)DBNull.Value;
                row["UnitId"] = order["unitid"] ?? (object)DBNull.Value;
                row["FleetId"] = order["fleetid"] ?? (object)DBNull.Value;
                row["CreatedDate"] = order["datecreate"] != DBNull.Value ? DateTime.Parse(order["datecreate"].ToString()) : (object)DBNull.Value;
                row["OrderType"] = order["OrderType"] ?? (object)DBNull.Value;
                row["OrderTotal"] = order["ordertotal"] ?? (object)DBNull.Value;
                row["isVoided"] = DBNull.Value; // Still no voided info
                row["InvoiceNumber"] = DBNull.Value; // You don't have invoiceslsid in your new query
                row["OrderNumber"] = DBNull.Value; // Still blank
                row["SalesPerson"] = DBNull.Value; // Still blank
                row["InvoiceType"] = DBNull.Value; // Still blank
                row["OrderCost"] = order["ordercost"] ?? (object)DBNull.Value;
                row["EmpId"] = order["servicewriter"] ?? (object)DBNull.Value; // If you want EmpId = ServiceWriter
                row["posted"] = order["posted"] ?? (object)DBNull.Value;
                row["BrnId"] = order["brnid"] ?? (object)DBNull.Value;
                row["estimate"] = order["estimate"] ?? (object)DBNull.Value;
                row["dateInvoice"] = order["dateinvoice"] != DBNull.Value ? DateTime.Parse(order["dateinvoice"].ToString()) : (object)DBNull.Value;
                row["LaborTotal"] = order["labortotal"] ?? (object)DBNull.Value;
                row["PartsTotal"] = order["partstotal"] ?? (object)DBNull.Value;
                row["LaborCostTotal"] = order["laborcosttotal"] ?? (object)DBNull.Value;
                row["PartsCostTotal"] = order["partscosttotal"] ?? (object)DBNull.Value;
                row["BillingCustomerID"] = (object)DBNull.Value; //Karmak Field as it is an integer
                row["BillingCustID"] = order["billCustomerid"] ?? (object)DBNull.Value;
                row["branch"] = order["brnid"] ?? (object)DBNull.Value;

                table.Rows.Add(row);
            }

            return table;
        }


        private DataTable MapOrdersPartsToDataTable(DataTable orders, int clientId)
        {
            var table = CreateExcedeOrderPartsDataTable();

            foreach (DataRow order in orders.Rows)
            {
                var row = table.NewRow();

                row["tnClientId"] = clientId;
                row["PartOrderItemID"] = order["id"]?.ToString() ?? (object)DBNull.Value;
                row["OrderId"] = order["slsid"]?.ToString() ?? (object)DBNull.Value;
                row["PartId"] = order["ptitm"]?.ToString() ?? (object)DBNull.Value;
                row["PartDescription"] = order["des"]?.ToString() ?? (object)DBNull.Value;
                row["QuantityShip"] = order["qtyship"] != DBNull.Value ? Convert.ToInt64(order["qtyship"]) : 0;
                row["QuantityBackOrder"] = order["qtybackorder"] != DBNull.Value ? Convert.ToInt64(order["qtybackorder"]) : 0;
                row["CreatedDate"] = order["datecreate"] != DBNull.Value ? DateTime.Parse(order["datecreate"].ToString()) : (object)DBNull.Value;
                row["OrderStatus"] = DBNull.Value; // Not available from ptslsitm
                row["Supplier"] = DBNull.Value; // Not available from ptslsitm
                row["AmtPrice"] = order["amtprice"] != DBNull.Value ? Convert.ToDecimal(order["amtprice"]) : (object)DBNull.Value;
                row["AmtCost"] = order["amtcost"] != DBNull.Value ? Convert.ToDecimal(order["amtcost"]) : (object)DBNull.Value;
                row["SrcId"] = DBNull.Value; // Requires lookup (like your Java code), can't set here directly
                row["AmtTax1"] = 0.00m; // Hardcoded
                row["AmtTax2"] = 0.00m; // Hardcoded
                row["VIN"] = DBNull.Value; // Not available
                row["Year"] = order["datecreate"] != DBNull.Value ? ((DateTime)order["datecreate"]).Year : (object)DBNull.Value;

                //row["Year"] = order["datecreate"] != DBNull.Value ? int.Parse(order["datecreate"].ToString().Substring(0, 4)) : (object)DBNull.Value;
                row["Manufacturer"] = DBNull.Value; // Not available
                row["itmTyp"] = order["itmtyp"] != DBNull.Value ? Convert.ToInt32(order["itmtyp"]) : (object)DBNull.Value;
                row["ItemTypeDesc"] = DBNull.Value; // Not available
                row["PartsActionType"] = DBNull.Value; // Not available
                row["PartType"] = DBNull.Value; // Not available
                row["Quantity"] = (order["qtyship"] != DBNull.Value ? Convert.ToDecimal(order["qtyship"]) : 0)
                                  + (order["qtybackorder"] != DBNull.Value ? Convert.ToDecimal(order["qtybackorder"]) : 0);



                table.Rows.Add(row);
            }

            return table;
        }

        public static string BuildClientOrdersQuery(Guid clientId, DateTime startDate, DateTime endDate)
        {
            string sql = $@"
            SELECT
                1 AS OrderType,
                s.cusid as customerid,
                s.client_id,
                s.amtcostparts + s.amtcostlabor AS ordercost,
                s.amtparts + s.amtlabor AS ordertotal,
                s.amtparts AS partstotal,
                s.amtlabor AS labortotal,
                s.amtcostparts AS partscosttotal,
                s.amtcostlabor AS laborcosttotal,
                s.brnid,
                s.status,
                s.estimate,
                s.datecreate,
                s.dateinvoice,
                s.billcusid AS billCustomerid,
                s.empidwriter AS servicewriter,
                s.untid AS unitid,
                s.fleetuntid AS fleetid,
                s.posted,
                s.id AS orderid
            FROM excede.svsls s
            WHERE s.client_id = '{clientId}'
              AND s.datecreate >= '{startDate:yyyy-MM-dd}'
              AND s.datecreate < '{endDate:yyyy-MM-dd}'

            UNION ALL

            SELECT
                2 AS OrderType,
                l.cusid as customerid,
                l.client_id,
                l.amtcost AS ordercost,
                l.amtsubtotal AS ordertotal,
                NULL AS partstotal,
                NULL AS labortotal,
                NULL AS partscosttotal,
                NULL AS laborcosttotal,
                l.brnid,
                l.status,
                NULL AS estimate,
                l.datecreate,
                NULL AS dateinvoice,
                l.billcusid AS billCustomerid,
                NULL AS servicewriter,
                NULL AS unitid,
                NULL AS fleetid,
                0 AS posted,
                l.id AS orderid
            FROM excede.lrcon l
            WHERE l.client_id = '{clientId}'
              AND l.datecreate >= '{startDate:yyyy-MM-dd}'
              AND l.datecreate < '{endDate:yyyy-MM-dd}'

            UNION ALL

            SELECT
                3 AS OrderType,
                p.cusid as customerid,
                p.client_id,
                p.amtcost AS ordercost,
                p.amtsubtotal AS ordertotal,
                NULL AS partstotal,
                NULL AS labortotal,
                NULL AS partscosttotal,
                NULL AS laborcosttotal,
                p.brnid,
                p.status,
                p.estimate,
                p.datecreate,
                p.dateinvoice,
                p.billcusid AS billCustomerid,
                NULL AS servicewriter,
                p.untid,
                NULL AS fleetid,
                p.posted,
                p.id AS orderid
            FROM excede.ptsls p
            WHERE p.client_id = '{clientId}'
              AND p.datecreate >= '{startDate:yyyy-MM-dd}'
              AND p.datecreate < '{endDate:yyyy-MM-dd}'
        ";

            return sql;
        }

        public static string BuildClientOrderPartsQuery(Guid clientId, DateTime startDate, DateTime endDate)
        {
            string sql = $@"
            SELECT
               *
            FROM excede.ptslsitm s
            WHERE s.client_id = '{clientId}'
              AND s.datecreate >= '{startDate:yyyy-MM-dd}'
              AND s.datecreate < '{endDate:yyyy-MM-dd}'
        ";

            return sql;
        }
    }

    

}
