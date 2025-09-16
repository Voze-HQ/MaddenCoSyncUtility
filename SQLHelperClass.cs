using System;
using System.Linq;
using System.Data;
using System.Data.SqlClient;

namespace MaddenCoSyncUtility
{
    class SQLHelperClass
    {
	  public SQLHelperClass()
	  {
		//
		// TODO: Add constructor logic here
		//
	  }
	  public SqlParameter SQLParameterHelper(string Name, SqlDbType Type, object Value)
	  {
		SqlParameter oP = new SqlParameter(Name, Type);
		oP.Value = Value;
		return oP;
	  }
    }
}
