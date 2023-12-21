package net.frontuari.lvecustomprocess.process;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.logging.Level;

import org.compiere.model.MAllocationHdr;
import org.compiere.model.MAllocationLine;
import org.compiere.model.MInvoice;
import org.compiere.process.DocAction;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.util.DB;
import org.compiere.util.Env;

import net.frontuari.lvecustomprocess.base.FTUProcess;

public class FTUInvoiceWriteOff extends FTUProcess{
	/**	BPartner				*/
	private int			p_C_BPartner_ID = 0;
	/** BPartner Group			*/
	private int			p_C_BP_Group_ID = 0;
	/**	Invoice					*/
	private int			p_C_Invoice_ID = 0;
	//**C_Currency_ID
	private int			p_C_Currency_ID = 0;
	
	/** Max Amt					*/
	private BigDecimal	p_MaxInvWriteOffAmt = Env.ZERO;
	/** AP or AR				*/
	private String		p_APAR = "R";
	private static String	ONLY_AP = "P";
	private static String	ONLY_AR = "R";
	
	/** Invoice Date From		*/
	private Timestamp	p_DateInvoiced_From = null;
	/** Invoice Date To			*/
	private Timestamp	p_DateInvoiced_To = null;
	/** Accounting Date			*/
	private Timestamp	p_DateAcct = null;
	/** Create Payment			*/
	private boolean		p_CreatePayment = false;
	/** Bank Account			*/
	private int			p_C_BankAccount_ID = 0;
	/** Simulation				*/
	private boolean		p_IsSimulation = true;
	
	private boolean p_IsSummary = false;

	/**	Allocation Hdr			*/
	private MAllocationHdr	m_alloc = null;
	/**	Payment					*/
	//private MPayment		m_payment = null;
	private int p_C_Charge_ID = 0;
	// Actual C_Bpartner
	private int actualC_BPartner_ID = 0;
	// Amount Total
	private BigDecimal TotalAmt = Env.ZERO;
	/**	Org Invoice				*/
	//	Added by Jorge Colmenarez, 2021-08-17 17:18
	private int			p_AD_Org_ID = 0;
	//	End Jorge Colmenarez
	/** PositiveOnly				*/
	private boolean		p_IsPositiveOnly = false;
	/**
	 *  Prepare - e.g., get Parameters.
	 */
	protected void prepare()
	{
		ProcessInfoParameter[] para = getParameter();
		for (int i = 0; i < para.length; i++)
		{
			String name = para[i].getParameterName();
			if (para[i].getParameter() == null && para[i].getParameter_To() == null)
				;
			else if (name.equals("C_BPartner_ID"))
				p_C_BPartner_ID = para[i].getParameterAsInt();
			else if (name.equals("C_BP_Group_ID"))
				p_C_BP_Group_ID = para[i].getParameterAsInt();
			else if (name.equals("C_Invoice_ID"))
				p_C_Invoice_ID = para[i].getParameterAsInt();
			else if (name.equals("C_Currency_ID"))
				p_C_Currency_ID = para[i].getParameterAsInt();
			//
			else if (name.equals("MaxInvWriteOffAmt"))
				p_MaxInvWriteOffAmt = (BigDecimal)para[i].getParameter();
			else if (name.equals("APAR"))
				p_APAR = (String)para[i].getParameter();
			//
			else if (name.equals("DateInvoiced"))
			{
				p_DateInvoiced_From = (Timestamp)para[i].getParameter();
				p_DateInvoiced_To = (Timestamp)para[i].getParameter_To();
			}
			else if (name.equals("DateAcct"))
				p_DateAcct = (Timestamp)para[i].getParameter();
			//
			else if (name.equals("CreatePayment"))
				p_CreatePayment = "Y".equals(para[i].getParameter());
			else if (name.equals("C_BankAccount_ID"))
				p_C_BankAccount_ID = para[i].getParameterAsInt();
			//
			else if (name.equals("IsSimulation"))
				p_IsSimulation = "Y".equals(para[i].getParameter());
			else if (name.equals("C_Charge_ID"))
				p_C_Charge_ID = para[i].getParameterAsInt();
			//	Added By Jorge Colmenarez, 2021-08-17 17:16
			else if (name.equals("AD_Org_ID"))
				p_AD_Org_ID = para[i].getParameterAsInt();
			else if (name.equalsIgnoreCase("IsSummary"))
				p_IsSummary = para[i].getParameterAsBoolean();
			//	End Jorge Colmenarez
			else if (name.equalsIgnoreCase("IsPositiveOnly"))
				p_IsPositiveOnly = para[i].getParameterAsBoolean();
			else
				log.log(Level.SEVERE, "Unknown Parameter: " + name);
		}
		
		
		if(p_DateAcct== null) {
			p_DateAcct = new Timestamp(System.currentTimeMillis());
		}
		
	}	//	prepare

	/**
	 * 	Execute
	 *	@return message
	 *	@throws Exception
	 */
	protected String doIt () throws Exception
	{
		if (log.isLoggable(Level.INFO)) log.info("C_BPartner_ID=" + p_C_BPartner_ID 
			+ ", C_BP_Group_ID=" + p_C_BP_Group_ID
			+ ", C_Invoice_ID=" + p_C_Invoice_ID
			+ "; APAR=" + p_APAR
			+ ", " + p_DateInvoiced_From + " - " + p_DateInvoiced_To
			+ "; CreatePayment=" + p_CreatePayment
			+ ", C_BankAccount_ID=" + p_C_BankAccount_ID);
		
		/*
		 * Added multi-currency support by: Armando Rojas
		 * */
		
		StringBuilder sql = new StringBuilder(
				"SELECT C_Invoice_ID,DocumentNo,DateInvoiced,")
				.append(" C_Currency_ID,GrandTotal,")
				.append("currencyconvert(invoiceOpen(C_Invoice_ID,0),c_currency_id,")
				.append(p_C_Currency_ID) // to Currency 
				.append(",dateinvoiced,c_conversiontype_id ,ad_client_id , ad_org_id ) AS OpenAmt")
				.append(" FROM C_Invoice WHERE ");
		
		sql.append(" 1=1 ");
		
		if (p_DateInvoiced_From != null && p_DateInvoiced_To != null) {
			sql.append(" AND TRUNC(DateInvoiced) BETWEEN ")
				.append(DB.TO_DATE(p_DateInvoiced_From, true))
				.append(" AND ")
				.append(DB.TO_DATE(p_DateInvoiced_To, true));
			
		}else if( p_DateInvoiced_From != null) {
			sql.append(" AND TRUNC(DateInvoiced) > ")
			.append(DB.TO_DATE(p_DateInvoiced_From, true));
		}else if( p_DateInvoiced_To != null) {
			
			sql.append(" AND TRUNC(DateInvoiced) < ")
			.append(DB.TO_DATE(p_DateInvoiced_To, true));
		}
		
		
		
		
		
		if (p_C_Invoice_ID != 0)
			sql.append(" AND C_Invoice_ID=").append(p_C_Invoice_ID);
		else
		{
			if (p_C_BPartner_ID != 0)
				sql.append(" AND C_BPartner_ID=").append(p_C_BPartner_ID);
			else if (p_C_BP_Group_ID > 0) {
				sql.append(" AND EXISTS (SELECT * FROM C_BPartner bp WHERE C_Invoice.C_BPartner_ID=bp.C_BPartner_ID AND bp.C_BP_Group_ID=")
					.append(p_C_BP_Group_ID).append(")");
			}
			
			// Do not filter by currency
			//if (p_C_Currency_ID>0)
			//	sql.append(" AND C_Currency_ID = " + p_C_Currency_ID);
			
			if (ONLY_AR.equals(p_APAR))
				sql.append(" AND IsSOTrx='Y'");
			else if (ONLY_AP.equals(p_APAR))
				sql.append(" AND IsSOTrx='N'");
			//
			//	Added By Jorge Colmenarez, 2021-08-17 17:19
			if(p_AD_Org_ID > 0)
				sql.append(" AND AD_Org_ID = ").append(p_AD_Org_ID);
			//	End Jorge Colmenarez
			
			sql.append(" AND currencyconvert(invoiceOpen(C_Invoice_ID,0),c_currency_id,")
			.append(p_C_Currency_ID) // to Currency 
			.append(",dateinvoiced,c_conversiontype_id ,ad_client_id , ad_org_id ) <= ")
			.append( p_MaxInvWriteOffAmt );
		}
		
		//Added by david castillo 10/11/2022
		//prevent negative writeoff
		if (p_IsPositiveOnly) {
			sql.append(" AND currencyconvert(invoiceOpen(C_Invoice_ID,0),c_currency_id,")
			.append(p_C_Currency_ID) // to Currency 
			.append(",dateinvoiced,c_conversiontype_id ,ad_client_id , ad_org_id ) >= 0");
		}
		
		sql.append(" AND IsPaid='N' AND DocStatus IN ('CO') ORDER BY AD_Org_ID, C_Currency_ID, C_BPartner_ID, DateInvoiced ");
		if (log.isLoggable(Level.FINER)) log.finer(sql.toString());
		
		//----------------------------------------------------------
		
		
		int counter = 0;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement (sql.toString(), get_TrxName());
			rs = pstmt.executeQuery ();
			while (rs.next ())
			{
				if (writeOff(rs.getInt(1), rs.getString(2), rs.getTimestamp(3),p_C_Currency_ID , rs.getBigDecimal(6)))
					counter++;
			}
		} 
		catch (Exception e)
		{
			log.log(Level.SEVERE, sql.toString(), e);
		}
		finally
		{
			DB.close(rs, pstmt);
			rs = null;
			pstmt = null;
		}
		//	final
		createChargeLine(TotalAmt, m_alloc, p_C_Charge_ID,actualC_BPartner_ID);
		processAllocation();
		StringBuilder msgreturn = new StringBuilder("#").append(counter);
		return msgreturn.toString();
	}	//	doIt

	/**
	 * 	Write Off
	 *	@param C_Invoice_ID invoice
	 *	@param DocumentNo doc no
	 *	@param DateInvoiced date
	 *	@param C_Currency_ID currency
	 *	@param OpenAmt open amt
	 *	@return true if written off
	 */
	private boolean writeOff (int C_Invoice_ID, String DocumentNo, Timestamp DateInvoiced, 
		int C_Currency_ID, BigDecimal OpenAmt)
	{
		// CONSOLE 
		
		System.out.println(  "Invoice ID = " + C_Invoice_ID);
		System.out.println(  "OpenAmt = " + OpenAmt);
		//	Nothing to do
		if (OpenAmt == null || OpenAmt.signum() == 0)
			return false;
		if (OpenAmt.abs().compareTo(p_MaxInvWriteOffAmt) >= 0)
			return false;
		//
		if (p_IsSimulation)
		{
			addLog(C_Invoice_ID, DateInvoiced, OpenAmt, DocumentNo);
			return true;
		}
		
		//	Invoice
		MInvoice invoice = new MInvoice(getCtx(), C_Invoice_ID, get_TrxName());
		if (!invoice.isSOTrx())
			OpenAmt = OpenAmt.negate();
		
		if (!p_IsSummary) {

			createChargeLine(TotalAmt, m_alloc, p_C_Charge_ID,actualC_BPartner_ID);
			processAllocation();
			m_alloc = new MAllocationHdr (getCtx(), true, 
				p_DateAcct, C_Currency_ID,
				getProcessInfo().getTitle() + " #" + getAD_PInstance_ID(), get_TrxName());
			m_alloc.setDateTrx(DateInvoiced);
			m_alloc.setAD_Org_ID(invoice.getAD_Org_ID());
			if (!m_alloc.save())
			{
				log.log(Level.SEVERE, "Cannot create allocation header");
				return false;
			}			
		}else {	
		
			//	Allocation
			if (m_alloc == null || C_Currency_ID != m_alloc.getC_Currency_ID() || m_alloc.getAD_Org_ID() != invoice.getAD_Org_ID() || actualC_BPartner_ID != invoice.getC_BPartner_ID() )
			{
				createChargeLine(TotalAmt, m_alloc, p_C_Charge_ID,actualC_BPartner_ID);
				processAllocation();
				m_alloc = new MAllocationHdr (getCtx(), true, 
					p_DateAcct, C_Currency_ID,
					getProcessInfo().getTitle() + " #" + getAD_PInstance_ID(), get_TrxName());
				m_alloc.setAD_Org_ID(invoice.getAD_Org_ID());
				if (!m_alloc.save())
				{
					log.log(Level.SEVERE, "Cannot create allocation header");
					return false;
				}
			}
		}

		//	Line
		MAllocationLine aLine = null;

		aLine = new MAllocationLine (m_alloc, OpenAmt, 
		Env.ZERO , Env.ZERO , Env.ZERO);
		aLine.setC_Invoice_ID(C_Invoice_ID);
		//aLine.setC_Charge_ID(p_C_Charge_ID);
		if (aLine.save())
		{
			addLog(C_Invoice_ID, DateInvoiced, OpenAmt, DocumentNo);
			actualC_BPartner_ID = invoice.getC_BPartner_ID();
			TotalAmt = TotalAmt.add(OpenAmt);
			return true;
		}
		//	Error
		log.log(Level.SEVERE, "Cannot create allocation line for C_Invoice_ID=" + C_Invoice_ID);
		return false;
	}	//	writeOff
	
	/**
	 * 	Process Allocation
	 *	@return true if processed
	 */
	private boolean processAllocation()
	{
		if (m_alloc == null)
			return true;
		
		//	Process It
		if (!m_alloc.processIt(DocAction.ACTION_Complete)) {
			log.warning("Allocation Process Failed: " + m_alloc + " - " + m_alloc.getProcessMsg());
			throw new IllegalStateException("Allocation Process Failed: " + m_alloc + " - " + m_alloc.getProcessMsg());
				
		}
		if (m_alloc.save()) {
			m_alloc = null;
			return true;
		}
		//
		m_alloc = null;
		return false;
	}	//	processAllocation

	/**
	 * 	Process Payment
	 *	@return true if processed
	 */
	
	private boolean createChargeLine(BigDecimal chargeAmt,MAllocationHdr alloc,int C_Charge_ID,int C_BPartner_ID){
		
		if (alloc != null && chargeAmt.compareTo(Env.ZERO) != 0) {
		MAllocationLine aLine = null;

		aLine = new MAllocationLine (m_alloc, chargeAmt, 
		Env.ZERO , Env.ZERO , Env.ZERO);
		//aLine.setC_Invoice_ID(C_Invoice_ID);
		aLine.setC_Charge_ID(C_Charge_ID);
		aLine.setC_BPartner_ID(C_BPartner_ID);
		if (aLine.save())
		{
			TotalAmt = Env.ZERO;
			return true;
		}
		//	Error
		log.log(Level.SEVERE, "Cannot create allocation line for C_CHARGE_ID=" + C_Charge_ID);
		return false;
		} return true;
	}

	

}
