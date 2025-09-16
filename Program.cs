using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;

namespace MaddenCoSyncUtility
{
    static class Program
    {
	  /// <summary>
	  /// The main entry point for the application.
	  /// </summary>
	  static void Main()
	  {
		MaddenCo oMC = new MaddenCo();
            DMSExcedeSync oDMS = new DMSExcedeSync();

            oMC.SendToElastic(385);
            oMC.syncMaddenCoDirectCustomers();

            //   oDMS.GetExcedeDMSOrderPartsData(Guid.Parse("e24d19b6-c1d3-4fac-bb16-dfbc42a3250c"), 1377); //Peterbilt of Atlanta
            ///   oDMS.GetExcedeDMSOrdersData(Guid.Parse("e24d19b6-c1d3-4fac-bb16-dfbc42a3250c"), 1377); //Peterbilt of Atlanta
            //oMC.GetMaddenCoCustomerRank();
            //oMC.GetMaddenCoCustomers(417);
            // oMC.GetMaddenCoData(1148);
            //oMC.GetMaddenCoData(385);
            //oMC.sendMaddenCoToElastic(385, "1/1/2025 08:00:00", "202501");
            //oMC.GetMaddenCoData(417);

            //oMC.GetMaddenCoCustomerList(417);

            //oMC.GetMaddenCoCustomerList(1217);


            //oMC.GetMaddenCoData_LinkedSQLServer(385);




            //oMC.GetMaddenCoSalesData(385);
            //oMC.GetMaddenCoData_LinkedSQLServer("202304", 385);
            //oMC.GetMaddenCoCustomerList(385);
            //        oMC.GetMaddenCoSalesData(417);
            //oMC.GetMaddenCoCustomerList(1366);
            //        oMC.GetMaddenCoCustomerList(1387);
            //        oMC.GetMaddenCoCustomerList(1334);
            //        oMC.GetMaddenCoCustomerList(417);



 //Prod Bauer oMC.GetMaddenCoData_LinkedSQLServer(385);
            // production oMC.sendCompanylabelsToElastic();

            //oMC.sendMaddenCoToElastic(385, "05/01/2023 08:00:00","202305");
            //oMC.sendCompanylabelsToElastic();

            //DateTime pDate = new DateTime(2023, 4, 1, 0, 0, 0);
            //  while (pDate < DateTime.Now)
            //   {
            //    string inv2Date = pDate.Month.ToString().PadLeft(2, '0') + @"/01/" + pDate.Year.ToString() + " 08:00:00";
            //    oMC.sendMaddenCoToElastic(385, inv2Date, pDate.Year.ToString() + pDate.Month.ToString().PadLeft(2, '0'));
            //    pDate = pDate.AddMonths(1);
            //  }

            oMC.sendCompanylabelsToElastic();

            //oMC.GetMaddenCoData_LinkedSQLServer("202001", 385);

            //--J  oMC.sendCompanylabelsToElastic();
            //oMC.sendMaddenCoToElastic(385, "12/1/2022 08:00:00", "202212");
            ///// oMC.sendCompanylabelsToElastic();


            //invDate = @"11/01/2018 08:00:00";
            //oMC.sendMaddenCoToElastic(385, invDate, "201811");

            //invDate = @"12/01/2018 08:00:00";
            //oMC.sendMaddenCoToElastic(385, invDate, "201812");



            //String invDate = @"01/01/2020 08:00:00";

            //DateTime pDate2 = new DateTime(2022, 7, 9, 0, 0, 0);
            //need to add this and build for Bauer.          //oMC.sendCompanylabelsToElastic();

            ////  while (pDate < DateTime.Now)
            ///   {
            //string invDate = pDate2.Month.ToString().PadLeft(2, '0') + @"/01/" + pDate2.Year.ToString() + " 08:00:00";
            //JF    oMC.sendMaddenCoToElastic(385, invDate, pDate2.Year.ToString() + pDate2.Month.ToString().PadLeft(2, '0'));
            //JF    pDate2 = pDate2.AddMonths(1);
            //  }





            //TSYS_SFTP oFTP = new TSYS_SFTP();
            //oFTP.GetFilesFromTSYS();
            //ServiceBase[] ServicesToRun;
            //ServicesToRun = new ServiceBase[] 
            //{ 
            //    new Service1() 
            //};
            //ServiceBase.Run(ServicesToRun);
        }
    }
}
