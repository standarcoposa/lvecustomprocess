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
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.logging.Level;

import org.compiere.model.MCommission;
import org.compiere.model.MCommissionAmt;
import org.compiere.model.MCommissionDetail;
import org.compiere.model.MCommissionLine;
import org.compiere.model.MCommissionRun;
import org.compiere.model.MConversionRate;
import org.compiere.model.MConversionType;
import org.compiere.model.MCurrency;
import org.compiere.model.MUser;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.util.AdempiereSystemError;
import org.compiere.util.AdempiereUserError;
import org.compiere.util.DB;
import org.compiere.util.DisplayType;
import org.compiere.util.Env;
import org.compiere.util.Language;

import net.frontuari.lvecustomprocess.base.FTUProcess;

/**
 *	Commission Calculation	
 *	
 *  @author Jorg Janke
 *  @version $Id: CommissionCalc.java,v 1.3 2006/09/25 00:59:41 jjanke Exp $
 */
public class FTUCommissionCalc extends FTUProcess
{
	private Timestamp		p_StartDate;
	//
	private Timestamp		m_EndDate;
	private MCommission		m_com;
	//
	private Timestamp 		p_DateFrom;
	private Timestamp 		p_DateTo;
	/**
	 *  Prepare - e.g., get Parameters.
	 */
	boolean useRange = false;
	protected void prepare()
	{
		ProcessInfoParameter[] para = getParameter();
		for (int i = 0; i < para.length; i++)
		{
			String name = para[i].getParameterName();
			if (para[i].getParameter() == null)
				;
			else if (name.equals("StartDate"))
				p_StartDate = (Timestamp)para[i].getParameter();
			else if (name.equals("DateFrom")) {
				p_DateFrom = para[i].getParameterAsTimestamp();
				p_DateTo = para[i].getParameter_ToAsTimestamp();
			}
			else if (name.equals("useRange"))
				useRange = para[i].getParameterAsBoolean();
			else
				log.log(Level.SEVERE, "Unknown Parameter: " + name);
		}
	}	//	prepare

	/**
	 *  Perform process.
	 *  @return Message (text with variables)
	 *  @throws Exception if not successful
	 */
	protected String doIt() throws Exception
	{
		if (log.isLoggable(Level.INFO)) log.info("C_Commission_ID=" + getRecord_ID() + ", StartDate=" + p_StartDate);
		if (p_StartDate == null)
			p_StartDate = new Timestamp (System.currentTimeMillis());
		m_com = new MCommission (getCtx(), getRecord_ID(), get_TrxName());
		if (m_com.get_ID() == 0)
			throw new AdempiereUserError ("No Commission");
			
		//	Create Commission	
		MCommissionRun comRun = new MCommissionRun (m_com);
		if (useRange) {
		comRun.setStartDate(p_DateFrom);
		p_StartDate = p_DateFrom;
		m_EndDate = p_DateTo;
		}else {
		setStartEndDate();
		comRun.setStartDate(p_StartDate);
		}
		//	01-Jan-2000 - 31-Jan-2001 - USD
		SimpleDateFormat format = DisplayType.getDateFormat(DisplayType.Date);
		StringBuilder description = new StringBuilder().append(format.format(p_StartDate)) 
			.append(" - ").append(format.format(m_EndDate))
			.append(" - ").append(MCurrency.getISO_Code(getCtx(), m_com.getC_Currency_ID()));
		comRun.setDescription(description.toString());
		if (!comRun.save())
			throw new AdempiereSystemError ("Could not save Commission Run");
		
		MCommissionLine[] lines = m_com.getLines();
		for (int i = 0; i < lines.length; i++)
		{
			//	Amt for Line - Updated By Trigger
			MCommissionAmt comAmt = new MCommissionAmt (comRun, lines[i].getC_CommissionLine_ID());
			if (!comAmt.save())
				throw new AdempiereSystemError ("Could not save Commission Amt");
			//
			StringBuilder sql = new StringBuilder();
			if (MCommission.DOCBASISTYPE_Receipt.equals(m_com.getDocBasisType()))
			{
				if (m_com.isListDetails())
				{
					sql.append("SELECT h.C_Currency_ID, MAX(CASE WHEN h.GrandTotal <> 0 ")
						.append(" 		THEN (l.LineNetAmt*al.Amount/h.GrandTotal) ")
						.append("		ELSE 0 END) AS Amt,")
						.append(" MAX(CASE WHEN h.GrandTotal <> 0 ")
						.append("		THEN (l.QtyInvoiced*al.Amount/h.GrandTotal) ")
						.append("		ELSE 0 END) AS Qty,")
						.append(" NULL, l.C_InvoiceLine_ID, string_agg(p.DocumentNo||'_'||h.DocumentNo , ' - ') as DocumentInfo,")
						.append(" COALESCE(prd.Value,l.Description) as productInfo, h.DateInvoiced ")
						.append(",MAX(currencyconvert(l.LineNetAmt,h.C_Currency_ID,?,h.dateInvoiced,?,h.AD_Client_ID,h.AD_Org_ID) - ")
						.append(" (COALESCE(x.total,0) / (SELECT COUNT(*) FROM C_InvoiceLine lp where lp.C_Invoice_ID = h.C_Invoice_ID))) as ConvertedAmt ")
						.append("FROM C_Payment p")
						.append(" INNER JOIN C_BankAccount ba on p.C_BankAccount_ID = ba.C_BankAccount_ID ")
						.append(" INNER JOIN C_Bank b on (ba.C_Bank_ID = b.C_Bank_ID AND b.IsWithholding in ('N'))")
						.append(" INNER JOIN C_AllocationLine al ON (p.C_Payment_ID=al.C_Payment_ID)")
						.append(" INNER JOIN C_AllocationHdr alh ON (al.C_AllocationHdr_ID = alh.C_AllocationHdr_ID)")
						.append(" INNER JOIN C_Invoice h ON (al.C_Invoice_ID = h.C_Invoice_ID)")
						.append(" INNER JOIN C_DocType dt ON (h.C_DocType_ID = dt.C_DocType_ID and dt.docbasetype in ('ARI'))")
						.append(" INNER JOIN C_InvoiceLine l ON (h.C_Invoice_ID = l.C_Invoice_ID) ")	
						.append(" LEFT  JOIN (SELECT COALESCE(SUM(currencyconvert(a.totallines,a.C_Currency_ID, ?,a.DateInvoiced,?,a.AD_Client_ID,a.AD_Org_ID) * CASE WHEN b.DocBaseType = 'ARI' THEN -1 ELSE 1 END),0) as total, a.LVE_invoiceAffected_ID as C_Invoice_ID FROM C_Invoice a ")
						.append(" JOIN C_DocType b on a.C_DocType_ID = b.C_DocType_ID WHERE a.DocStatus = 'CO' GROUP BY 2) x on x.C_Invoice_ID = h.C_Invoice_ID ")
						.append(" LEFT OUTER JOIN M_Product prd ON (l.M_Product_ID = prd.M_Product_ID) ")
						.append("WHERE p.DocStatus IN ('CL','CO')")
						.append(" AND h.IsSOTrx='Y'")
						.append(" AND p.AD_Client_ID = ?")
						.append(" AND p.DateAcct BETWEEN ? AND ?")
						.append(" AND invoiceopen(h.C_Invoice_ID,null) <= 0")
						.append(" AND l.C_InvoiceLine_ID NOT IN (SELECT C_InvoiceLine_ID FROM C_CommissionDetail cd ")
						.append(" JOIN C_CommissionAmt ca ON cd.C_CommissionAmt_ID = ca.C_CommissionAmt_ID ")
						.append(" JOIN C_CommissionRun cr ON cr.C_CommissionRun_ID = ca.C_CommissionRun_ID ")
						.append(" JOIN C_Commission c on cr.C_Commission_ID = c.C_Commission_ID ")
						.append(" WHERE c.C_BPartner_ID = " + m_com.getC_BPartner_ID() + ")")
						.append(" AND P.DateAcct - h.DateDelivered <= interval '" +m_com.get_ValueAsInt("DaysDue") + " days'");
						
				}
				else
				{
					sql.append("SELECT h.C_Currency_ID, ")
						.append(" SUM(CASE WHEN h.GrandTotal <> 0 ")
						.append("		THEN l.LineNetAmt*al.Amount/h.GrandTotal ELSE 0 END) AS Amt,")
						.append(" SUM(CASE WHEN h.GrandTotal <> 0 ")
						.append("		THEN l.QtyInvoiced*al.Amount/h.GrandTotal  ELSE 0 END) AS Qty,")
						.append(" NULL, NULL, NULL, NULL, MAX(h.DateInvoiced) ")
						.append(",currencyconvert(l.LineNetAmt,h.C_Currency_ID,?,h.dateInvoiced,?,h.AD_Client_ID,h.AD_Org_ID) - ")
						.append(" COALESCE(x.total,0) as ConvertedAmt ")
						.append("FROM C_Payment p")
						.append(" INNER JOIN C_BankAccount ba on p.C_BankAccount_ID = ba.C_BankAccount_ID ")
						.append(" INNER JOIN C_Bank b on (ba.C_Bank_ID = b.C_Bank_ID AND b.IsWithholding in ('N'))")
						.append(" INNER JOIN C_AllocationLine al ON (p.C_Payment_ID=al.C_Payment_ID)")
						.append(" INNER JOIN C_AllocationHdr alh ON (al.C_AllocationHdr_ID = alh.C_AllocationHdr_ID)")
						.append(" INNER JOIN C_Invoice h ON (al.C_Invoice_ID = h.C_Invoice_ID)")
						.append(" INNER JOIN C_DocType dt ON (h.C_DocType_ID = dt.C_DocType_ID and dt.docbasetype in ('ARI'))")
						.append(" INNER JOIN C_InvoiceLine l ON (h.C_Invoice_ID = l.C_Invoice_ID) ")
						.append(" LEFT  JOIN (SELECT COALESCE(SUM(currencyconvert(a.totallines,a.C_Currency_ID, ?,a.DateInvoiced,?,a.AD_Client_ID,a.AD_Org_ID) * CASE WHEN b.DocBaseType = 'ARI' THEN -1 ELSE 1 END),0) as total, a.LVE_invoiceAffected_ID as C_Invoice_ID FROM C_Invoice a ")
						.append(" JOIN C_DocType b on a.C_DocType_ID = b.C_DocType_ID WHERE a.DocStatus = 'CO' GROUP BY 2) x on x.C_Invoice_ID = h.C_Invoice_ID ")
						.append("WHERE p.DocStatus IN ('CL','CO')")
						.append(" AND h.IsSOTrx='Y'")
						.append(" AND p.AD_Client_ID = ?")
						.append(" AND p.DateTrx BETWEEN ? AND ?")
						.append(" AND invoiceopen(h.C_Invoice_ID,null) <= 0")
						.append(" AND P.DateTrx - h.DateDelivered <= interval '" +m_com.get_ValueAsInt("DaysDue") + " days'");
						
				}
			}	
			else if (MCommission.DOCBASISTYPE_Order.equals(m_com.getDocBasisType()))
			{
				if (m_com.isListDetails())
				{
					sql.append("SELECT h.C_Currency_ID, l.LineNetAmt, l.QtyOrdered, ")
						.append("l.C_OrderLine_ID, NULL, h.DocumentNo,")
						.append(" COALESCE(prd.Value,l.Description),h.DateOrdered ")
						.append(", currencyconvert(l.LineNetAmt,h.C_Currency_ID,?,h.DateOrdered,?,h.AD_Client_ID,h.AD_Org_ID) AS ConvertedAmt ")
						.append("FROM C_Order h")
						.append(" INNER JOIN C_OrderLine l ON (h.C_Order_ID = l.C_Order_ID)")
						.append(" LEFT OUTER JOIN M_Product prd ON (l.M_Product_ID = prd.M_Product_ID) ")
						.append("WHERE h.DocStatus IN ('CL','CO')")
						.append(" AND h.IsSOTrx='Y'")
						.append(" AND h.AD_Client_ID = ?")
						.append(" AND h.DateOrdered BETWEEN ? AND ?");
				}
				else
				{
					sql.append("SELECT h.C_Currency_ID, SUM(l.LineNetAmt) AS Amt,")
						.append(" SUM(l.QtyOrdered) AS Qty, ")
						.append("NULL, NULL, NULL, NULL, MAX(h.DateOrdered) ")
						.append(", sum(currencyconvert(l.LineNetAmt,h.C_Currency_ID,?,h.DateOrdered,?,h.AD_Client_ID,h.AD_Org_ID)) AS ConvertedAmt ")
						.append("FROM C_Order h")
						.append(" INNER JOIN C_OrderLine l ON (h.C_Order_ID = l.C_Order_ID) ")
						.append("WHERE h.DocStatus IN ('CL','CO')")
						.append(" AND h.IsSOTrx='Y'")
						.append(" AND h.AD_Client_ID = ?")
						.append(" AND h.DateOrdered BETWEEN ? AND ?");
				}
			}
			else 	//	Invoice Basis
			{
				if (m_com.isListDetails())
				{
					sql.append("SELECT h.C_Currency_ID, l.LineNetAmt, l.QtyInvoiced, ")
						.append("NULL, l.C_InvoiceLine_ID, h.DocumentNo,")
						.append(" COALESCE(prd.Value,l.Description),h.DateInvoiced ")
						.append(", currencyconvert(l.LineNetAmt,h.C_Currency_ID,?,h.DateInvoiced,?,h.AD_Client_ID,h.AD_Org_ID) AS ConvertedAmt ")
						.append("FROM C_Invoice h")
						.append(" INNER JOIN C_DocType dt ON (h.C_DocType_ID = dt.C_DocType_ID and dt.C_DocTypeDeclare = '01')")
						.append(" INNER JOIN C_InvoiceLine l ON (h.C_Invoice_ID = l.C_Invoice_ID)")
						.append(" LEFT OUTER JOIN M_Product prd ON (l.M_Product_ID = prd.M_Product_ID) ")
						.append(" LEFT  JOIN (SELECT COALESCE(SUM(currencyconvert(a.grandtotal,a.C_Currency_ID, ?,a.DateInvoiced,?,a.AD_Client_ID,a.AD_Org_ID) * CASE WHEN b.DocBaseType = 'ARI' THEN -1 ELSE 1 END),0) as total, a.LVE_invoiceAffected_ID as C_Invoice_ID FROM C_Invoice a ")
						.append(" JOIN C_DocType b on a.C_DocType_ID = b.C_DocType_ID WHERE a.DocStatus = 'CO' GROUP BY 2) x on x.C_Invoice_ID = h.C_Invoice_ID ")
						.append("WHERE h.DocStatus IN ('CL','CO')")
						.append(" AND h.IsSOTrx='Y'")
						.append(" AND h.AD_Client_ID = ?")
						.append(" AND h.DateDelivered BETWEEN ? AND ?")
						.append(" AND invoiceopen(h.C_Invoice_ID,null) = 0");
				}
				else
				{
					sql.append("SELECT h.C_Currency_ID, SUM(l.LineNetAmt) AS Amt,")
						.append(" SUM(l.QtyInvoiced) AS Qty, ")
						.append("NULL, NULL, NULL, NULL, MAX(h.DateInvoiced) ")
						.append(", sum(currencyconvert(l.LineNetAmt,h.C_Currency_ID,?,h.DateInvoiced,?,h.AD_Client_ID,h.AD_Org_ID))-x.total AS ConvertedAmt ")
						.append("FROM C_Invoice h")
						.append(" INNER JOIN C_DocType dt ON (h.C_DocType_ID = dt.C_DocType_ID and dt.C_DocTypeDeclare = '01')")
						.append(" INNER JOIN C_InvoiceLine l ON (h.C_Invoice_ID = l.C_Invoice_ID) ")
						.append(" LEFT  JOIN (SELECT COALESCE(SUM(currencyconvert(a.grandtotal,a.C_Currency_ID, ?,a.DateInvoiced,?,a.AD_Client_ID,a.AD_Org_ID) * CASE WHEN b.DocBaseType = 'ARI' THEN -1 ELSE 1 END),0) as total, a.LVE_invoiceAffected_ID as C_Invoice_ID FROM C_Invoice a ")
						.append(" JOIN C_DocType b on a.C_DocType_ID = b.C_DocType_ID WHERE a.DocStatus = 'CO' GROUP BY 2) x on x.C_Invoice_ID = h.C_Invoice_ID ")
						.append("WHERE h.DocStatus IN ('CL','CO')")
						.append(" AND h.IsSOTrx='Y'")
						.append(" AND h.AD_Client_ID = ?")
						.append(" AND h.DateDelivered BETWEEN ? AND ?")
						.append(" AND invoiceopen(h.C_Invoice_ID,null) = 0");
				}
			}
			//	CommissionOrders/Invoices
			if (lines[i].isCommissionOrders())
			{
				MUser[] users = MUser.getOfBPartner(getCtx(), m_com.getC_BPartner_ID(), get_TrxName());
				if (users == null || users.length == 0)
					throw new AdempiereUserError ("Commission Business Partner has no Users/Contact");
				if (users.length == 1)
				{
					int SalesRep_ID = users[0].getAD_User_ID();
					sql.append(" AND h.SalesRep_ID=").append(SalesRep_ID);
				}
				else
				{
					log.warning("Not 1 User/Contact for C_BPartner_ID=" 
						+ m_com.getC_BPartner_ID() + " but " + users.length);
					sql.append(" AND h.SalesRep_ID IN (SELECT AD_User_ID FROM AD_User WHERE C_BPartner_ID=")
						.append(m_com.getC_BPartner_ID()).append(")");
				}
			}
			//	Organization
			if (lines[i].getOrg_ID() != 0)
				sql.append(" AND h.AD_Org_ID=").append(lines[i].getOrg_ID());
			//	BPartner
			if (lines[i].getC_BPartner_ID() != 0)
				sql.append(" AND h.C_BPartner_ID=").append(lines[i].getC_BPartner_ID());
			//	BPartner Group
			if (lines[i].getC_BP_Group_ID() != 0)
				sql.append(" AND h.C_BPartner_ID IN ")
					.append("(SELECT C_BPartner_ID FROM C_BPartner WHERE C_BP_Group_ID=").append(lines[i].getC_BP_Group_ID()).append(")");
			//	Sales Region
			if (lines[i].getC_SalesRegion_ID() != 0)
				sql.append(" AND h.C_BPartner_Location_ID IN ")
					.append("(SELECT C_BPartner_Location_ID FROM C_BPartner_Location WHERE C_SalesRegion_ID=").append(lines[i].getC_SalesRegion_ID()).append(")");
			//	Product
			if (lines[i].getM_Product_ID() != 0)
				sql.append(" AND l.M_Product_ID=").append(lines[i].getM_Product_ID());
			//	Product Category
			if (lines[i].getM_Product_Category_ID() != 0)
				sql.append(" AND l.M_Product_ID IN ")
					.append("(SELECT M_Product_ID FROM M_Product WHERE M_Product_Category_ID=").append(lines[i].getM_Product_Category_ID()).append(")");
			//	Payment Rule
			if (lines[i].getPaymentRule() != null)
				sql.append(" AND h.PaymentRule='").append(lines[i].getPaymentRule()).append("'");
			//	Grouping
			if (!m_com.isListDetails())
				sql.append(" GROUP BY h.C_Currency_ID");
			if (MCommission.DOCBASISTYPE_Receipt.equals(m_com.getDocBasisType()) && m_com.isListDetails()){
				sql.append(" GROUP BY h.C_Currency_ID , l.C_InvoiceLine_ID,ProductInfo,h.dateinvoiced ");
			}
			//
			if (log.isLoggable(Level.FINE)) log.fine("Line=" + lines[i].getLine() + " - " + sql);
			//
			createDetail(sql.toString(), comAmt);
			comAmt = calculateCommission(comAmt);
			comAmt.saveEx();
		}	//	for all commission lines
		
	//	comRun.updateFromAmt();
	//	comRun.saveEx();
		
		//	Save Last Run
		m_com.setDateLastRun (p_StartDate);
		m_com.saveEx();
		StringBuilder msgreturn = new StringBuilder("@C_CommissionRun_ID@ = ").append(comRun.getDocumentNo()) 
				.append(" - ").append(comRun.getDescription());
		return msgreturn.toString();
	}	//	doIt

	/**
	 * 	Set Start and End Date
	 */
	private void setStartEndDate()
	{
		GregorianCalendar cal = new GregorianCalendar(Language.getLoginLanguage().getLocale());
		cal.setTimeInMillis(p_StartDate.getTime());
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		//	Yearly
		if (MCommission.FREQUENCYTYPE_Yearly.equals(m_com.getFrequencyType()))
		{
			cal.set(Calendar.DAY_OF_YEAR, 1);
			p_StartDate = new Timestamp (cal.getTimeInMillis());
			//
			cal.add(Calendar.YEAR, 1);
			cal.add(Calendar.DAY_OF_YEAR, -1); 
			m_EndDate = new Timestamp (cal.getTimeInMillis());
			
		}
		//	Quarterly
		else if (MCommission.FREQUENCYTYPE_Quarterly.equals(m_com.getFrequencyType()))
		{
			cal.set(Calendar.DAY_OF_MONTH, 1);
			int month = cal.get(Calendar.MONTH);
			if (month < Calendar.APRIL)
				cal.set(Calendar.MONTH, Calendar.JANUARY);
			else if (month < Calendar.JULY)
				cal.set(Calendar.MONTH, Calendar.APRIL);
			else if (month < Calendar.OCTOBER)
				cal.set(Calendar.MONTH, Calendar.JULY);
			else
				cal.set(Calendar.MONTH, Calendar.OCTOBER);
			p_StartDate = new Timestamp (cal.getTimeInMillis());
			//
			cal.add(Calendar.MONTH, 3);
			cal.add(Calendar.DAY_OF_YEAR, -1); 
			m_EndDate = new Timestamp (cal.getTimeInMillis());
		}
		//	Weekly
		else if (MCommission.FREQUENCYTYPE_Weekly.equals(m_com.getFrequencyType()))
		{
			cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
			p_StartDate = new Timestamp (cal.getTimeInMillis());
			//
			cal.add(Calendar.DAY_OF_YEAR, 7); 
			m_EndDate = new Timestamp (cal.getTimeInMillis());
		}
		//	Monthly
		else
		{
			cal.set(Calendar.DAY_OF_MONTH, 1);
			p_StartDate = new Timestamp (cal.getTimeInMillis());
			//
			cal.add(Calendar.MONTH, 1);
			cal.add(Calendar.DAY_OF_YEAR, -1); 
			m_EndDate = new Timestamp (cal.getTimeInMillis());
		}
		if (log.isLoggable(Level.FINE)) log.fine("setStartEndDate = " + p_StartDate + " - " + m_EndDate);
		
		/**
		String sd = DB.TO_DATE(p_StartDate, true);
		StringBuffer sql = new StringBuffer ("SELECT ");
		if (MCommission.FREQUENCYTYPE_Quarterly.equals(m_com.getFrequencyType()))
			sql.append("TRUNC(").append(sd).append(", 'Q'), TRUNC(").append(sd).append("+92, 'Q')-1");
		else if (MCommission.FREQUENCYTYPE_Weekly.equals(m_com.getFrequencyType()))
			sql.append("TRUNC(").append(sd).append(", 'DAY'), TRUNC(").append(sd).append("+7, 'DAY')-1");
		else	//	Month
			sql.append("TRUNC(").append(sd).append(", 'MM'), TRUNC(").append(sd).append("+31, 'MM')-1");
		sql.append(" FROM DUAL");
		**/
	}	//	setStartEndDate

	/**
	 * 	Create Commission Detail
	 *	@param sql sql statement
	 *	@param comAmt parent
	 * @throws Exception 
	 */
	private void createDetail (String sql, MCommissionAmt comAmt) throws Exception
	{
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			
			pstmt = DB.prepareStatement(sql, get_TrxName());
			if (MCommission.DOCBASISTYPE_Receipt.equals(m_com.getDocBasisType())){
				pstmt.setInt(1, m_com.getC_Currency_ID());
				pstmt.setInt(2, (Integer) m_com.get_Value("C_ConversionType_ID"));
				pstmt.setInt(3, m_com.getC_Currency_ID());
				pstmt.setInt(4, (Integer) m_com.get_Value("C_ConversionType_ID"));
				pstmt.setInt(5, m_com.getAD_Client_ID());
				pstmt.setTimestamp(6, p_StartDate);
				pstmt.setTimestamp(7, m_EndDate);
		
			}else if(MCommission.DOCBASISTYPE_Invoice.equals(m_com.getDocBasisType())) {
				pstmt.setInt(1, m_com.getC_Currency_ID());
				pstmt.setInt(2, (Integer) m_com.get_Value("C_ConversionType_ID"));
				pstmt.setInt(3, m_com.getC_Currency_ID());
				pstmt.setInt(4, (Integer) m_com.get_Value("C_ConversionType_ID"));
				pstmt.setInt(5, m_com.getAD_Client_ID());
				pstmt.setTimestamp(6, p_StartDate);
				pstmt.setTimestamp(7, m_EndDate);
		
			}
			
			else{
				pstmt.setInt(1, m_com.getC_Currency_ID());
				pstmt.setInt(2, (Integer) m_com.get_Value("C_ConversionType_ID"));
				pstmt.setInt(3, m_com.getAD_Client_ID());
				pstmt.setTimestamp(4, p_StartDate);
				pstmt.setTimestamp(5, m_EndDate);
			}
			log.log(Level.SEVERE, pstmt.toString());
			rs = pstmt.executeQuery();
			while (rs.next())
			{
				
			
				//	CommissionAmount, C_Currency_ID, Amt, Qty,
				MCommissionDetail cd = new MCommissionDetail (comAmt,
					rs.getInt(1), rs.getBigDecimal(2), rs.getBigDecimal(3));
					
				//	C_OrderLine_ID, C_InvoiceLine_ID,
				cd.setLineIDs(rs.getInt(4), rs.getInt(5));
				
				//	Reference, Info,
				String s = rs.getString(6);
				if (s != null)
					cd.setReference(s);
				s = rs.getString(7);
				if (s != null)
					cd.setInfo(s);
				
				//	Date
				Timestamp date = rs.getTimestamp(8);
				
				//cd.setConvertedAmt(date);
				/*BigDecimal ConvertedAmt = MConversionRate.convert(getCtx(),  rs.getBigDecimal(2), rs.getInt(1),m_com.getC_Currency_ID() ,
						ConvDate, (Integer) m_com.get_Value("C_ConversionType_ID"), getAD_Client_ID(), m_com.getAD_Org_ID()); calculations are going to be done on sql*/
				//
				cd.setConvertedAmt(rs.getBigDecimal(9));
				MCommissionLine cl = (MCommissionLine) comAmt.getC_CommissionLine();
				BigDecimal amt = cd.getConvertedAmt().multiply(cl.getAmtMultiplier()).setScale(4, RoundingMode.HALF_UP);
				cd.set_ValueOfColumn("CommissionAmt", amt);
				if (!cd.save())
					throw new IllegalArgumentException ("CommissionCalc - Detail Not saved");
			}
		}
		catch (Exception e)
		{
			throw new AdempiereSystemError("System Error: " + e.getLocalizedMessage(), e);
		}
		finally
		{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
	}	//	createDetail
	
	public MCommissionAmt calculateCommission(MCommissionAmt comAmt)
	{
		MCommissionDetail[] details = comAmt.getDetails();
		BigDecimal ConvertedAmt = Env.ZERO;
		BigDecimal ActualQty = Env.ZERO;
		for (int i = 0; i < details.length; i++)
		{
			MCommissionDetail detail = details[i];
			BigDecimal amt = detail.getConvertedAmt();
			if (amt == null)
				amt = Env.ZERO;
			ConvertedAmt = ConvertedAmt.add(amt);
			ActualQty = ActualQty.add(detail.getActualQty());
		}
		comAmt.setConvertedAmt(ConvertedAmt);
		comAmt.setActualQty(ActualQty);
		//
		MCommissionLine cl = new MCommissionLine(getCtx(),comAmt.getC_CommissionLine_ID(), get_TrxName());
		//	Qty
		BigDecimal qty = comAmt.getActualQty().subtract(cl.getQtySubtract());
		if (cl.isPositiveOnly() && qty.signum() < 0)
			qty = Env.ZERO;
		qty = qty.multiply(cl.getQtyMultiplier());
		//	Amt
		BigDecimal amt = comAmt.getConvertedAmt().multiply(cl.getAmtMultiplier());
		
		amt = amt.subtract(cl.getAmtSubtract());
		if (cl.isPositiveOnly() && amt.signum() < 0)
			amt = Env.ZERO;
		//
		comAmt.setCommissionAmt(amt);
		return comAmt;
	}	//	calculateCommission
	

}	//	CommissionCalc
