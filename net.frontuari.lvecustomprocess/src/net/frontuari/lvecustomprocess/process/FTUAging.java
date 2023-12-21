/******************************************************************************
 * Product: Adempiere ERP & CRM Smart Business Solution                       *
 * Copyright (C) 1999-2006 ComPiere, Inc. All Rights Reserved.                *
 * This program is free software; you can redistribute it and/or modify it    *
 * under the terms version 2 of the GNU General Public License as published   *
 * by the Free Software Foundation. This program is distributed in the hope   *
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied *
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.           *
 * See the GNU General Public License for more details.                       *
 * You should have received a copy of the GNU General Public License along    *
 * with this program; if not, write to the Free Software Foundation, Inc.,    *
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.                     *
 * For the text or an alternative of this public license, you may reach us    *
 * ComPiere, Inc., 2620 Augustine Dr. #245, Santa Clara, CA 95054, USA        *
 * or via info@compiere.org or http://www.compiere.org/license.html           *
 *****************************************************************************/
package net.frontuari.lvecustomprocess.process;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.logging.Level;

import org.adempiere.exceptions.DBException;
import org.compiere.model.MInvoice;
import org.compiere.model.MPayment;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.TimeUtil;

import net.frontuari.lvecustomprocess.base.FTUProcess;
import net.frontuari.lvecustomprocess.model.MFTUAging;

/**
 *	Invoice Aging Report.
 *	Based on RV_Aging.
 *  @author Jorg Janke
 *  @author victor.perez@e-evolution.com  FR 1933937  Is necessary a new Aging to Date
 *  @see http://sourceforge.net/tracker/index.php?func=detail&aid=1933937&group_id=176962&atid=879335 
 *  @author Carlos Ruiz - globalqss  BF 2655587  Multi-org not supported in Aging
 *  @see https://sourceforge.net/tracker2/?func=detail&aid=2655587&group_id=176962&atid=879332 
 *  @version $Id: Aging.java,v 1.5 2006/10/07 00:58:44 jjanke Exp $
 *  @author Jorge Colmenarez - Frontuari, C.A.	Add DueAmt and PastDueAmt 0<=21 Days, PaymentTerm, DocumentNo, GrandTotal
 *  @author Jose Ruiz - Frontuari, C.A.	Add C_DocType_ID , C_DocTypeTarget_ID
 */
public class FTUAging extends FTUProcess
{
	/** The date to calculate the days due from			*/
	private Timestamp	p_StatementDate = null;
	//FR 1933937
	private boolean		p_DateAcct = false;
	private boolean 	p_IsSOTrx = false;
	private int			p_ConvertCurrencyTo_ID = 0;
	private int			p_AD_Org_ID = 0;
	private int			p_C_BP_Group_ID = 0;
	private int			p_C_BPartner_ID = 0;
	private boolean		p_IsListInvoices = false;
	/** Number of days between today and statement date	*/
	private int			m_statementOffset = 0;
	private boolean 	p_ShowAcct = false;
	//added by david castillo  10/02/2022 
	private String		p_FTU_BPartnerType_ID = null;
	
	/**
	 *  Prepare - e.g., get Parameters.
	 */
	protected void prepare()
	{
		ProcessInfoParameter[] para = getParameter();
		for (int i = 0; i < para.length; i++)
		{
			String name = para[i].getParameterName();
			if (para[i].getParameter() == null)
				;
			else if (name.equals("StatementDate"))
				p_StatementDate = (Timestamp)para[i].getParameter();
			else if (name.equals("DateAcct"))
				p_DateAcct = "Y".equals(para[i].getParameter());
			else if (name.equals("IsSOTrx"))
				p_IsSOTrx = "Y".equals(para[i].getParameter());
			else if (name.equals("C_Currency_ID"))
				;
			else if (name.equals("ConvertAmountsInCurrency_ID"))
				p_ConvertCurrencyTo_ID = para[i].getParameterAsInt();
			else if (name.equals("AD_Org_ID"))
				p_AD_Org_ID = ((BigDecimal)para[i].getParameter()).intValue();
			else if (name.equals("C_BP_Group_ID"))
				p_C_BP_Group_ID = ((BigDecimal)para[i].getParameter()).intValue();
			else if (name.equals("C_BPartner_ID"))
				p_C_BPartner_ID = ((BigDecimal)para[i].getParameter()).intValue();
			else if (name.equals("FTU_BPartnerType_ID"))
				p_FTU_BPartnerType_ID = para[i].getParameterAsString();
			else if (name.equals("IsListInvoices"))
				p_IsListInvoices = "Y".equals(para[i].getParameter());
			else if (name.equals("IsShowAcct"))
				p_ShowAcct = "Y".equals(para[i].getParameter());
			else
				log.log(Level.SEVERE, "Unknown Parameter: " + name);
		}
		if (p_StatementDate == null)
			p_StatementDate = new Timestamp (System.currentTimeMillis());
		else
			m_statementOffset = TimeUtil.getDaysBetween( 
				new Timestamp(System.currentTimeMillis()), p_StatementDate);
	}	//	prepare 

	/**
	 * 	DoIt
	 *	@return Message
	 *	@throws Exception
	 */
	protected String doIt() throws Exception
	{
		if (log.isLoggable(Level.INFO)) log.info("StatementDate=" + p_StatementDate + ", IsSOTrx=" + p_IsSOTrx
			+ ", ConvertAmountsInCurrency_ID=" + p_ConvertCurrencyTo_ID + ", AD_Org_ID=" + p_AD_Org_ID
			+ ", C_BP_Group_ID=" + p_C_BP_Group_ID + ", C_BPartner_ID=" + p_C_BPartner_ID
			+ ", IsListInvoices=" + p_IsListInvoices);
		//FR 1933937
		String dateacct = DB.TO_DATE(p_StatementDate);  
		 
		StringBuilder sql = new StringBuilder();
		//	Open declaration Query for UNIon 
		sql.append("SELECT * FROM (");
		sql.append("select bp.C_BP_Group_ID, oi.C_BPartner_ID,oi.C_Invoice_ID,oi.C_InvoicePaySchedule_ID, "  // 1..4 
			+ "oi.C_Currency_ID, oi.IsSOTrx, "								//	5..6
			+ "oi.DateInvoiced, oi.NetDays,oi.DueDate,oi.DaysDue, ");		//	7..10
		if (p_ConvertCurrencyTo_ID == 0)
		{
			if (!p_DateAcct)//FR 1933937
			{
				sql.append(" oi.GrandTotal, oi.PaidAmt, oi.OpenAmt ");			//	11..13
			}
			else
			{
				sql.append(" oi.GrandTotal, invoicePaidToDate(oi.C_Invoice_ID, oi.C_Currency_ID, 1,"+dateacct+") AS PaidAmt, invoiceOpenToDate(oi.C_Invoice_ID,oi.C_InvoicePaySchedule_ID,"+dateacct+") AS OpenAmt ");			//	11..13
			}
		}
		else
		{
			String s = ",oi.C_Currency_ID," + p_ConvertCurrencyTo_ID + ",oi.DateInvoiced,oi.C_ConversionType_ID,oi.AD_Client_ID,oi.AD_Org_ID)";
			sql.append("currencyConvert(oi.GrandTotal").append(s);		//	11
			if (!p_DateAcct)
			{
				sql.append(", currencyConvert(oi.PaidAmt").append(s)  // 12
				.append(", currencyConvert(oi.OpenAmt").append(s);  // 13
			}
			else
			{
				sql.append(", currencyConvert(invoicePaidToDate(oi.C_Invoice_ID, oi.C_Currency_ID, 1,"+dateacct+")").append(s) // 12
				.append(", currencyConvert(invoiceOpenToDate(oi.C_Invoice_ID,oi.C_InvoicePaySchedule_ID,"+dateacct+")").append(s);  // 13
			}
		}
		sql.append(",oi.C_Activity_ID,oi.C_Campaign_ID,oi.C_Project_ID,oi.AD_Org_ID ");	//	14..17
		//	Added Support for get DocumentNo,C_PaymentTerm_ID
		sql.append(",oi.DocumentNo,oi.C_PaymentTerm_ID,oi.DateAcct AS DateDoc ");	//	18..20
		//	Add CurrencyRate
		if (p_ConvertCurrencyTo_ID == 0)
		{
			sql.append(",currencyRate(oi.C_Currency_ID,"+Env.getContext(getCtx(), "$C_Currency_ID")+",oi.DateInvoiced,oi.C_ConversionType_ID,oi.AD_Client_ID,oi.AD_Org_ID,true) AS Rate,oi.C_DocType_ID,oi.C_DocTypeTarget_ID "); // 21
		}
		else
		{
			sql.append(",currencyRate(oi.C_Currency_ID,"+p_ConvertCurrencyTo_ID+",oi.DateInvoiced,oi.C_ConversionType_ID,oi.AD_Client_ID,oi.AD_Org_ID) AS Rate ,oi.C_DocType_ID,oi.C_DocTypeTarget_ID "); // 21
		}
		//Added by David Castillo 22-10-2021
		//Add B_PartnerValue, Sales Sector, docType, PrePayment, openAmt, $OpenAmt, creditDays, Status, Account plus		 some other shit that arichuna wants
		sql.append(" ,bp.Value as BPartnerValue ,dt.Name as DocTypeName, ao.Name as SalesSector, oi.openamtdollars, ");//24....27
		sql.append(" sr.Name as SalesRegionValue, oi.SalesRep_ID, ");//28-29
		sql.append(" coalesce(currencyRate(oi.C_Currency_ID,100,oi.dateinvoiced,oi.C_ConversionType_ID,oi.AD_Client_ID,oi.AD_Org_ID,true),0) AS CurrencyRate");//30
		if (p_ShowAcct)
			sql.append(" , ev.value || ' - ' || ev.Name as Account_Name");	//31
		
		sql.append(",oi.AD_Client_ID,'N' AS IsPrepayment ");	//32....33		
		if (!p_DateAcct)//FR 1933937
		{
			sql.append(" from RV_OpenItem oi");
		}
		else
		{
			sql.append(" from RV_OpenItemToDate oi");
		}
		
		sql.append(" join C_BPartner bp on oi.C_BPartner_ID = bp.C_BPartner_ID ");
		//Add Joins to tables
		//doctype
		sql.append(" join C_DocType dt on oi.C_DocType_ID = dt.C_DocType_ID ");
		//FTU_BPartnerType
		sql.append(" join AD_Org ao on oi.AD_Org_ID = ao.AD_Org_ID");
		//sales rep
		//sql.append(" join AD_User au on oi.SalesRep_ID = au.AD_User_ID");
		//Sales Region
		sql.append(" join C_BPartner_Location cbl on oi.C_BPartner_Location_ID = cbl.C_BPartner_Location_ID");
		sql.append(" left join C_SalesRegion sr on cbl.C_SalesRegion_ID = sr.C_SalesRegion_ID ");
		//accounting
		
		if (p_ShowAcct) {
					sql.append(" left join Fact_Acct fa on fa.AD_Table_ID = "+MInvoice.Table_ID+" and fa.Record_ID = oi.C_Invoice_ID AND"
							+ " fa.Line_ID IS NULL AND fa.C_Tax_ID IS NULL AND fa.C_LocFrom_ID IS NOT NULL AND fa.C_AcctSchema_ID = "+Env.getContext(getCtx(), "$C_AcctSchema_ID"));
					sql.append(" AND (CASE WHEN '"+(p_IsSOTrx ? "Y" : "N")+"' = 'Y' AND substring(dt.DocBaseType,3) = 'C' THEN fa.AmtAcctCr  > 0  ");
					sql.append(" WHEN '"+(p_IsSOTrx ? "Y" : "N")+"' = 'Y' AND substring(dt.DocBaseType,3) != 'C' THEN fa.AmtAcctDr  > 0  ");
					sql.append(" WHEN '"+(p_IsSOTrx ? "Y" : "N")+"' = 'N' AND substring(dt.DocBaseType,3) = 'C' THEN fa.AmtAcctDr  > 0  ");
					sql.append(" WHEN '"+(p_IsSOTrx ? "Y" : "N")+"' = 'N' AND substring(dt.DocBaseType,3) != 'C' THEN fa.AmtAcctCr  > 0 END)");
					sql.append(" left join C_ElementValue ev on fa.Account_ID = ev.C_ElementValue_ID");
				}
		sql.append(" where oi.ISSoTrx=").append(p_IsSOTrx ? "'Y'" : "'N'");
		sql.append(" and oi.AD_Client_ID = ").append(getAD_Client_ID());
		if (p_C_BPartner_ID > 0)
		{
			sql.append(" and oi.C_BPartner_ID=").append(p_C_BPartner_ID);
		}
		else if (p_C_BP_Group_ID > 0)
		{
			sql.append(" and bp.C_BP_Group_ID=").append(p_C_BP_Group_ID);
		}
		if (p_AD_Org_ID > 0) // BF 2655587
		{
			sql.append(" and oi.AD_Org_ID=").append(p_AD_Org_ID);
		}
		//add bpartnertype 10/02/2022
		if (p_FTU_BPartnerType_ID != null)
		{
			sql.append(" and bp.FTU_BPartnerType_ID IN (").append(p_FTU_BPartnerType_ID).append(")");	
			
		}
		if (p_DateAcct)//FR 1933937
		{
			sql.append(" and invoiceOpenToDate(oi.C_Invoice_ID,oi.C_InvoicePaySchedule_ID,"+dateacct+") <> 0 ");
		}
		
		//	Added by Jorge Colmenarez, 2021-10-25 18:19 
		//	Add Pre-Payment rows
		sql.append(" union all ");
		sql.append("select bp.C_BP_Group_ID, oi.C_BPartner_ID,oi.C_Payment_ID AS C_Invoice_ID,0 AS C_InvoicePaySchedule_ID, "  // 1..4 
				+ "oi.C_Currency_ID, oi.IsReceipt AS IsSOTrx, "								//	5..6
				+ "oi.DateTrx AS DateInvoiced, 0 AS NetDays,oi.DateTrx AS DueDate,0 AS DaysDue, ");		//	7..10
			if (p_ConvertCurrencyTo_ID == 0)
			{
				if (!p_DateAcct)//FR 1933937
				{
					sql.append(" oi.PayAmt * (CASE WHEN oi.IsReceipt = 'Y' THEN -1 ELSE 1 END) AS GrandTotal, (oi.PayAmt-COALESCE(paymentavailable(oi.C_Payment_ID),0)) * (CASE WHEN oi.IsReceipt = 'Y' THEN -1 ELSE 1 END) AS PaidAmt, COALESCE(paymentavailable(oi.C_Payment_ID),0) * (CASE WHEN oi.IsReceipt = 'Y' THEN -1 ELSE 1 END) AS OpenAmt ");			//	11..13
				}
				else
				{
					sql.append(" oi.PayAmt * (CASE WHEN oi.IsReceipt = 'Y' THEN -1 ELSE 1 END) AS GrandTotal, (oi.PayAmt-COALESCE(paymentavailabletodate(oi.C_Payment_ID,"+dateacct+"),0)) * (CASE WHEN oi.IsReceipt = 'Y' THEN -1 ELSE 1 END) AS PaidAmt, COALESCE(paymentavailabletodate(oi.C_Payment_ID,"+dateacct+"),0) * (CASE WHEN oi.IsReceipt = 'Y' THEN -1 ELSE 1 END) AS OpenAmt ");			//	11..13
				}
			}
			else
			{
				String s = ",oi.C_Currency_ID," + p_ConvertCurrencyTo_ID + ",oi.DateTrx,oi.C_ConversionType_ID,oi.AD_Client_ID,oi.AD_Org_ID)";
				sql.append("currencyConvert(oi.PayAmt * (CASE WHEN oi.IsReceipt = 'Y' THEN -1 ELSE 1 END)").append(s);		//	11
				if (!p_DateAcct)
				{
					sql.append(", currencyConvert((oi.PayAmt-COALESCE(paymentavailable(oi.C_Payment_ID),0)) * (CASE WHEN oi.IsReceipt = 'Y' THEN -1 ELSE 1 END)").append(s)  // 12
					.append(", currencyConvert(COALESCE(paymentavailable(oi.C_Payment_ID),0) * (CASE WHEN oi.IsReceipt = 'Y' THEN -1 ELSE 1 END)").append(s);  // 13
				}
				else
				{
					sql.append(", currencyConvert(COALESCE(paymentavailabletodate(oi.C_Payment_ID,"+dateacct+"),0) * (CASE WHEN oi.IsReceipt = 'Y' THEN -1 ELSE 1 END)").append(s) // 12
					.append(", currencyConvert(COALESCE(paymentavailabletodate(oi.C_Payment_ID,"+dateacct+"),0) * (CASE WHEN oi.IsReceipt = 'Y' THEN -1 ELSE 1 END)").append(s);  // 13
				}
			}
			sql.append(",oi.C_Activity_ID,oi.C_Campaign_ID,oi.C_Project_ID,oi.AD_Org_ID ");	//	14..17
			//	Added Support for get DocumentNo,C_PaymentTerm_ID
			sql.append(",oi.DocumentNo,0 AS C_PaymentTerm_ID,oi.DateAcct AS DateDoc ");	//	18..20
			//	Add CurrencyRate
			if (p_ConvertCurrencyTo_ID == 0)
			{
				sql.append(",currencyRate(oi.C_Currency_ID,"+Env.getContext(getCtx(), "$C_Currency_ID")+",oi.DateTrx,oi.C_ConversionType_ID,oi.AD_Client_ID,oi.AD_Org_ID,true) AS Rate,oi.C_DocType_ID,oi.C_DocType_ID AS C_DocTypeTarget_ID "); // 21
			}
			else
			{
				sql.append(",currencyRate(oi.C_Currency_ID,"+p_ConvertCurrencyTo_ID+",oi.DateTrx,oi.C_ConversionType_ID,oi.AD_Client_ID,oi.AD_Org_ID) AS Rate ,oi.C_DocType_ID,oi.C_DocType_ID AS C_DocTypeTarget_ID "); // 21
			}
			//Added by David Castillo 22-10-2021
			//Add B_PartnerValue, Sales Sector, docType, PrePayment, openAmt, $OpenAmt, creditDays, Status, Account.
			sql.append(" ,bp.Value as BPartnerValue ,dt.Name as DocTypeName, ao.Name as SalesSector, currencyConvert(COALESCE(paymentavailable(oi.C_Payment_ID) * (CASE WHEN oi.IsReceipt = 'Y' THEN -1 ELSE 1 END),0),oi.C_Currency_ID,100,oi.DateTrx,oi.C_ConversionType_ID,oi.AD_Client_ID,oi.AD_Org_ID) AS openamtdollars, ");//24....27
			sql.append(" ' ' as SalesRegionValue, oi.SalesRep_ID, ");//28 - 29
			sql.append(" coalesce(currencyRate(oi.C_Currency_ID,100,oi.DateTrx,oi.C_ConversionType_ID,oi.AD_Client_ID,oi.AD_Org_ID,true),0) AS CurrencyRate");//30
			if (p_ShowAcct)
				sql.append(" , ev.value || ' - ' || ev.Name as Account_Name");	//31
			
			sql.append(",oi.AD_Client_ID,'Y' AS IsPrepayment ");	//32....33
			

			sql.append(" from C_Payment oi");
			
			sql.append(" join C_BPartner bp on oi.C_BPartner_ID=bp.C_BPartner_ID ");
			//Add Joins to tables
			//doctype
			sql.append(" join C_DocType dt on oi.C_DocType_ID = dt.C_DocType_ID ");
			//FTU_BPartnerType
			sql.append(" join AD_Org ao on oi.AD_Org_ID = ao.AD_Org_ID ");
			//sales rep
			//sql.append(" join AD_User au on oi.SalesRep_ID = au.AD_User_ID");
		
			//accounting
			if (p_ShowAcct) {
						sql.append(" left join Fact_Acct fa on fa.AD_Table_ID = "+MPayment.Table_ID+" and fa.Record_ID = oi.C_Payment_ID AND (CASE '"+(p_IsSOTrx ? "Y" : "N")+"' WHEN 'Y' THEN fa.AmtAcctCr > 0 ELSE fa.AmtAcctDr > 0 END) AND fa.C_AcctSchema_ID = "+Env.getContext(getCtx(), "$C_AcctSchema_ID"));
						sql.append(" left join C_ElementValue ev on fa.Account_ID = ev.C_ElementValue_ID");
					}
			sql.append(" where oi.DocStatus in ('CO','CL') and oi.IsReceipt=").append(p_IsSOTrx ? "'Y'" : "'N'");
			sql.append(" and oi.AD_Client_ID = ").append(getAD_Client_ID());
			if (p_C_BPartner_ID > 0)
			{
				sql.append(" and oi.C_BPartner_ID=").append(p_C_BPartner_ID);
			}
			else if (p_C_BP_Group_ID > 0)
			{
				sql.append(" and bp.C_BP_Group_ID=").append(p_C_BP_Group_ID);
			}
			if (p_AD_Org_ID > 0) // BF 2655587
			{
				sql.append(" and oi.AD_Org_ID=").append(p_AD_Org_ID);
			}
			//add bpartnertype 10/02/2022		
			if (p_FTU_BPartnerType_ID != null)
			{
				sql.append(" and bp.FTU_BPartnerType_ID IN (").append(p_FTU_BPartnerType_ID).append(")");	
				
			}
			if (p_DateAcct)
			{
				sql.append(" and COALESCE(paymentavailabletodate(oi.C_Payment_ID,"+dateacct+"),0) <> 0 ");
			}
			else 
			{
				sql.append(" and COALESCE(paymentavailable(oi.C_Payment_ID),0) <> 0 ");
			}
		//	Close declaration query for UNIon 
		sql.append(") oi ");
		sql.append(" order by oi.C_BPartner_ID, oi.C_Currency_ID, oi.C_Invoice_ID");
		
		/*if (log.isLoggable(Level.FINEST)) log.finest(sql.toString());
		String finalSql = MRole.getDefault(getCtx(), false).addAccessSQL(
			sql.toString(), "oi", MRole.SQL_FULLYQUALIFIED, MRole.SQL_RO);	
		log.finer(finalSql);*/

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		//
		MFTUAging aging = null;
		int counter = 0;
		int rows = 0;
		int AD_PInstance_ID = getAD_PInstance_ID();
		//
		try
		{
			pstmt = DB.prepareStatement(sql.toString(), get_TrxName());
			rs = pstmt.executeQuery();
			while (rs.next())
			{
				int C_BP_Group_ID = rs.getInt(1);	
				int C_BPartner_ID = rs.getInt(2);
				int C_Invoice_ID = p_IsListInvoices ? rs.getInt(3) : 0;
				int C_InvoicePaySchedule_ID = p_IsListInvoices ? rs.getInt(4) : 0;
				int C_Currency_ID = rs.getInt(5);
				boolean IsSOTrx = "Y".equals(rs.getString(6));
				//
				//Timestamp DateInvoiced = rs.getTimestamp(7);
				int NetDays = rs.getInt(8);
				Timestamp DueDate = rs.getTimestamp(9);
				//	Days Due
				int DaysDue = rs.getInt(10)		//	based on today
					+ m_statementOffset;
				//
				BigDecimal GrandTotal = rs.getBigDecimal(11);
				//BigDecimal PaidAmt = rs.getBigDecimal(12);
				BigDecimal OpenAmt = rs.getBigDecimal(13);
				//
				int C_Activity_ID = p_IsListInvoices ? rs.getInt(14) : 0;
				int C_Campaign_ID = p_IsListInvoices ? rs.getInt(15) : 0;
				int C_Project_ID = p_IsListInvoices ? rs.getInt(16) : 0;
				int AD_Org_ID = rs.getInt(17);
				
				String DocumentNo = p_IsListInvoices ? rs.getString(18) : "";
				int C_PaymentTerm_ID = p_IsListInvoices ? rs.getInt(19) : 0;
				Timestamp DateInvoiced = p_IsListInvoices ? rs.getTimestamp(7) : null;
				Timestamp DateDoc = p_IsListInvoices ? rs.getTimestamp(20) : null;

				BigDecimal Rate = rs.getBigDecimal(21);
				
				int C_DocType_ID = p_IsListInvoices ? rs.getInt(22) : 0;
				int C_DocTypeTarget_ID = p_IsListInvoices ? rs.getInt(23) : 0;
				//
				
				String BPartnerValue =  rs.getString(24);
				String DocTypeName = rs.getString(25);
				String SalesSector = rs.getString(26);
				BigDecimal openAmtDollars = rs.getBigDecimal(27);
				String SalesRegionValue = rs.getString(28);
				int SalesRep_ID = rs.getInt(29);
				BigDecimal CurrencyRate = rs.getBigDecimal(30);
				
				String Account_Name = p_ShowAcct ? rs.getString(31) : " ";
				BigDecimal prepayAmt = p_ShowAcct ? rs.getBoolean(33) ? rs.getBigDecimal(13) : BigDecimal.ZERO : rs.getBoolean(32) ? rs.getBigDecimal(13) : BigDecimal.ZERO;  
				rows++;
				boolean isPrepayment = p_ShowAcct ? rs.getBoolean(33) : rs.getBoolean(32);
				//	New Aging Row
				if (aging == null 		//	Key
					|| AD_PInstance_ID != aging.getAD_PInstance_ID()
					|| C_BPartner_ID != aging.getC_BPartner_ID()
					|| C_Currency_ID != aging.getC_Currency_ID()
					|| C_Invoice_ID != aging.getC_Invoice_ID()
					|| C_InvoicePaySchedule_ID != aging.getC_InvoicePaySchedule_ID())
				{
					if (aging != null)
					{
						aging.saveEx();
						if (log.isLoggable(Level.FINE)) log.fine("#" + ++counter + " - " + aging);
					}
					aging = new MFTUAging (getCtx(), AD_PInstance_ID, p_StatementDate, 
						C_BPartner_ID, C_Currency_ID, 
						C_Invoice_ID, C_InvoicePaySchedule_ID, 
						C_BP_Group_ID, AD_Org_ID, DueDate, IsSOTrx, get_TrxName());
					aging.setC_Activity_ID(C_Activity_ID);
					aging.setC_Campaign_ID(C_Campaign_ID);
					aging.setC_Project_ID(C_Project_ID);
					aging.setDateAcct(p_DateAcct);
					aging.setConvertAmountsInCurrency_ID(p_ConvertCurrencyTo_ID);
					//	Set DocumentNo,C_PaymentTerm_ID,GrandTotal
					aging.set_ValueOfColumn("DocumentNo", DocumentNo);
					aging.set_ValueOfColumn("C_PaymentTerm_ID", C_PaymentTerm_ID);
					aging.set_ValueOfColumn("GrandTotal", GrandTotal);
					aging.set_ValueOfColumn("DateInvoiced", DateInvoiced);
					aging.set_ValueOfColumn("DateDoc", DateDoc);
					aging.set_ValueOfColumn("C_DocType_ID", C_DocType_ID);
					aging.set_ValueOfColumn("C_DocTypeTarget_ID", C_DocTypeTarget_ID); 
					//add new Values
					aging.set_ValueOfColumn("Rate", Rate);
					aging.set_ValueOfColumn("DocTypeName", DocTypeName);
					aging.set_ValueOfColumn("SalesSector", SalesSector);
					aging.set_ValueOfColumn("openAmtDollars", openAmtDollars);
					aging.set_ValueOfColumn("Account_Name", Account_Name);
					aging.set_ValueOfColumn("BPartnerValue", BPartnerValue);
					aging.set_ValueOfColumn("NetDays", NetDays);
					aging.set_ValueOfColumn("PrepaidAmt", prepayAmt);
					aging.set_ValueOfColumn("SalesRegionValue",SalesRegionValue);
					aging.set_ValueOfColumn("CurrencyRate", CurrencyRate);
					aging.set_ValueOfColumn("SalesRep_ID",SalesRep_ID );
				}
				//	Fill Buckets
				if (!isPrepayment) {
					aging.add (DueDate, DaysDue, GrandTotal, OpenAmt);	
				}else {
					if (prepayAmt != null) 				
						aging.set_ValueOfColumn("OpenAmt",OpenAmt);
				}
				
			}
			if (aging != null)
			{
				aging.saveEx();
				counter++;
				if (log.isLoggable(Level.FINE)) log.fine("#" + counter + " - " + aging);
			}
		}
		catch (SQLException e)
		{
			throw new DBException(e, sql.toString());
		}
		finally
		{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		//	
		if (log.isLoggable(Level.INFO)) log.info("#" + counter + " - rows=" + rows);
		return "";
	}	//	doIt

}	//	Aging
