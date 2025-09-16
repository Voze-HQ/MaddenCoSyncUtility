using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;


namespace MaddenCoSyncUtility
{
    class SQL
    {
        public DataTable GetSalesSummary(int clientid, string InvDate)
        {
            SQLHelperClass oHelper = new SQLHelperClass();
            SQLConnect oSQL = new SQLConnect();
            string cSQL = "client.getSalesSummary_3";
            string TableName = "tSel";


            SqlParameter[] oaryP = new SqlParameter[2];
            DataSet oDS = new DataSet();

            oaryP[0] = oHelper.SQLParameterHelper("@clientid", SqlDbType.Int, clientid);
            oaryP[1] = oHelper.SQLParameterHelper("@invoicedate", SqlDbType.DateTime, InvDate);

            oDS = oSQL.SQLPT(oDS, TableName, cSQL, oaryP, "Telenotes", true);
            return oDS.Tables[TableName];

        }

		public DataTable GetMaddenCoData(int clientid, string InvDate)
		{
			SQLHelperClass oHelper = new SQLHelperClass();
			SQLConnect oSQL = new SQLConnect();
			string cSQL = "INTEGRATIONS.SyncMaddenCo_local"; //+		nMonthPrev	{1/11/2025 12:00:00 AM}	System.DateTime

			string TableName = "tSel";


			SqlParameter[] oaryP = new SqlParameter[2];
			DataSet oDS = new DataSet();

			oaryP[0] = oHelper.SQLParameterHelper("@clientid", SqlDbType.Int, clientid);
			oaryP[1] = oHelper.SQLParameterHelper("@SyncDate", SqlDbType.Char, InvDate);

			oDS = oSQL.SQLPT(oDS, TableName, cSQL, oaryP, "Telenotes", true);
			return oDS.Tables[TableName];

		}

		public bool ResetSalesSummary(int CustomerID, string invdt)
	  {
		SQLHelperClass oHelper = new SQLHelperClass();
		SQLConnect oSQL = new SQLConnect();
		string cSQL = "spRESETSalesSummary_new";
		string TableName = "tSel";

		SqlParameter[] oaryP = new SqlParameter[2];
		DataSet oDS = new DataSet();

		oaryP[0] = oHelper.SQLParameterHelper("@clientid", SqlDbType.Int, CustomerID);
		oaryP[1] = oHelper.SQLParameterHelper("@mdinvdt", SqlDbType.Char, invdt);
		
		oDS = oSQL.SQLPT(oDS, TableName, cSQL, oaryP, "Telenotes", true);
		return true;

	  }
        public int AddCustomer(String CompanyName, String AccountId, int ClientId, String Address1, String City, String State, String Zip, String Phone, String Fax, String ContactFN, String ContactLN, String PrimaryPhone, String SalesId)
        {
            SQLHelperClass oHelper = new SQLHelperClass();
            SQLConnect oSQL = new SQLConnect();
            string cSQL = "integrations.AddAcCompany";
            string TableName = "tSel";

            SqlParameter[] oaryP = new SqlParameter[13];
            DataSet oDS = new DataSet();

            oaryP[0] = oHelper.SQLParameterHelper("@CompanyName", SqlDbType.NVarChar, CompanyName.Trim());
            oaryP[1] = oHelper.SQLParameterHelper("@AccountId", SqlDbType.NVarChar, AccountId.Trim());
            oaryP[2] = oHelper.SQLParameterHelper("@ClientId", SqlDbType.Int, ClientId);
            oaryP[3] = oHelper.SQLParameterHelper("@Address1", SqlDbType.NVarChar, Address1.Trim());
            oaryP[4] = oHelper.SQLParameterHelper("@City", SqlDbType.NVarChar, City.Trim());
            oaryP[5] = oHelper.SQLParameterHelper("@State", SqlDbType.NVarChar, State.Trim());
            oaryP[6] = oHelper.SQLParameterHelper("@Zip", SqlDbType.NVarChar, Zip.Trim());
            oaryP[7] = oHelper.SQLParameterHelper("@Phone", SqlDbType.NVarChar, Phone.Trim());
            oaryP[8] = oHelper.SQLParameterHelper("@Fax", SqlDbType.NVarChar, Fax.Trim());
            oaryP[9] = oHelper.SQLParameterHelper("@ContactFN", SqlDbType.NVarChar, ContactFN.Trim());
            oaryP[10] = oHelper.SQLParameterHelper("@ContactLN", SqlDbType.NVarChar, ContactLN.Trim());
            oaryP[11] = oHelper.SQLParameterHelper("@PrimaryPhone", SqlDbType.NVarChar, PrimaryPhone.Trim());
            oaryP[12] = oHelper.SQLParameterHelper("@SalesId", SqlDbType.NVarChar, SalesId.Trim());

            
            oDS = oSQL.SQLPT(oDS, TableName, cSQL, oaryP, "Telenotes", true);
            
            return Convert.ToInt32(oDS.Tables[TableName].Rows[0]["RetVal"].ToString());

        }
	  public int AddCustomerRank(int CustomerID, string CustomerName,double Units,double RepCost,double Sales,double FedTaxProd, double FedTax, int SalesManId, DateTime InvoiceDate)
	  {
		


    SQLHelperClass oHelper = new SQLHelperClass();
		SQLConnect oSQL = new SQLConnect();
		string cSQL = "spIMPCustRankData";
		string TableName = "tSel";

		SqlParameter[] oaryP = new SqlParameter[9];
		DataSet oDS = new DataSet();

		oaryP[0] = oHelper.SQLParameterHelper("@CustomerID", SqlDbType.Int, CustomerID);
		oaryP[1] = oHelper.SQLParameterHelper("@CustomerName", SqlDbType.NVarChar, CustomerName);
		oaryP[2] = oHelper.SQLParameterHelper("@Units", SqlDbType.Decimal, Units);
		oaryP[3] = oHelper.SQLParameterHelper("@RepCost", SqlDbType.Decimal, RepCost);
		oaryP[4] = oHelper.SQLParameterHelper("@Sales", SqlDbType.Decimal, Sales);
		oaryP[5] = oHelper.SQLParameterHelper("@FedTaxProd", SqlDbType.Decimal, FedTaxProd);
		oaryP[6] = oHelper.SQLParameterHelper("@FedTax", SqlDbType.Decimal, FedTax);
		oaryP[7] = oHelper.SQLParameterHelper("@SalesManId", SqlDbType.Int, SalesManId);
		oaryP[8] = oHelper.SQLParameterHelper("@InvoiceDate", SqlDbType.DateTime, InvoiceDate);

		oDS = oSQL.SQLPT(oDS, TableName, cSQL, oaryP, "Telenotes", true);
		return Convert.ToInt32(oDS.Tables[TableName].Rows[0]["RetID"].ToString());
	  }
	  public int AddSalesSummary(string invoice, DateTime invoiceDate, string productName, string str, int lct, string description, double qty,
		double price, string fet, double extension, bool isadd, double cost, double profit, int salespersonId, string productId, int customerId, 
		int clientId, string sINVDT, SqlConnection oConnection)
	  {



		SQLHelperClass oHelper = new SQLHelperClass();
		SQLConnect oSQL = new SQLConnect();
		string cSQL = "spIMPSalesSummary_v2";
		string TableName = "tSel";

		SqlParameter[] oaryP = new SqlParameter[18];
		DataSet oDS = new DataSet();

		oaryP[0] = oHelper.SQLParameterHelper("@Invoice", SqlDbType.NVarChar, invoice);
		oaryP[1] = oHelper.SQLParameterHelper("@InvoiceDate ", SqlDbType.DateTime, invoiceDate);
		oaryP[2] = oHelper.SQLParameterHelper("@ProductName", SqlDbType.VarChar, productName);
		oaryP[3] = oHelper.SQLParameterHelper("@STR", SqlDbType.VarChar, str);
		oaryP[4] = oHelper.SQLParameterHelper("@LCT", SqlDbType.Int, lct);
		oaryP[5] = oHelper.SQLParameterHelper("@Description", SqlDbType.VarChar, description);
		oaryP[6] = oHelper.SQLParameterHelper("@Qty", SqlDbType.Decimal, qty);
		oaryP[7] = oHelper.SQLParameterHelper("@Price", SqlDbType.Decimal, price);
		oaryP[8] = oHelper.SQLParameterHelper("@Fet", SqlDbType.Char, fet);
		oaryP[9] = oHelper.SQLParameterHelper("@Extension", SqlDbType.Decimal, extension);
		oaryP[10] = oHelper.SQLParameterHelper("@IsAdd", SqlDbType.Bit, isadd);
		oaryP[11] = oHelper.SQLParameterHelper("@Cost", SqlDbType.Decimal, cost);
		oaryP[12] = oHelper.SQLParameterHelper("@Profit", SqlDbType.Decimal, profit);
		oaryP[13] = oHelper.SQLParameterHelper("@SalesPersonID", SqlDbType.Int, salespersonId);
		oaryP[14] = oHelper.SQLParameterHelper("@ProductID", SqlDbType.NVarChar, productId);
		oaryP[15] = oHelper.SQLParameterHelper("@CustomerID", SqlDbType.Int, customerId);
		oaryP[16] = oHelper.SQLParameterHelper("@clientId", SqlDbType.Int, clientId);
		oaryP[17] = oHelper.SQLParameterHelper("@mdinvdt", SqlDbType.Char, sINVDT);


		oDS = oSQL.SQLPT(oDS, TableName, cSQL, oaryP, "Telenotes", true,oConnection);
		return Convert.ToInt32(oDS.Tables[TableName].Rows[0]["RetID"].ToString());

	  }
    }

}
