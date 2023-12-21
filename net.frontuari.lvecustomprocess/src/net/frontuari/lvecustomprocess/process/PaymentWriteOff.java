package net.frontuari.lvecustomprocess.process;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.logging.Level;

import org.compiere.model.MAllocationHdr;
import org.compiere.model.MAllocationLine;

import org.compiere.model.MPayment;
import org.compiere.process.DocAction;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.util.DB;
import org.compiere.util.Env;

import net.frontuari.lvecustomprocess.base.FTUProcess;

public class PaymentWriteOff extends FTUProcess{

	/**	BPartner				*/
	private int			p_C_BPartner_ID = 0;
	/** BPartner Group			*/
	private int			p_C_BP_Group_ID = 0;
	/**	Invoice					*/
	private int			p_C_Payment_ID = 0;
	//**C_Currency_ID
	private int			p_C_Currency_ID = 0;
	/** Max Amt					*/
	private BigDecimal	p_MaxInvWriteOffAmt = Env.ZERO;
	/** AP or AR				*/
	private boolean p_IsReceipt = false;
	private boolean p_IsSummary = false;
	private String		p_APAR = "R";
	private static String	ONLY_AP = "P";
	private static String	ONLY_AR = "R";
	/** Invoice Date From		*/
	private Timestamp	p_DateTrx_From = null;
	/** Invoice Date To			*/
	private Timestamp	p_DateTrx_To = null;
	/** Accounting Date			*/
	private Timestamp	p_DateAcct = null;
	/** Create Payment			*/
	private boolean		p_CreatePayment = false;
	/** Bank Account			*/
	private int			p_C_BankAccount_ID = 0;
	/** Simulation				*/
	private boolean		p_IsSimulation = true;

	/**	Allocation Hdr			*/
	private MAllocationHdr	m_alloc = null;
	/**	Payment					*/
	
	private int p_C_Charge_ID = 0;
	// Actual C_Bpartner
	private int actualC_BPartner_ID = 0;
	// Amount Total
	private BigDecimal TotalAmt = Env.ZERO;
	
	/** PositiveOnly				*/
	private boolean		p_IsPositiveOnly = false;
	/**
	 *  Prepare - e.g., get Parameters.
	 */
	@Override
	protected void prepare() {
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
			else if (name.equals("C_Payment_ID"))
				p_C_Payment_ID = para[i].getParameterAsInt();
			//
			else if (name.equals("C_Currency_ID"))
				p_C_Currency_ID = para[i].getParameterAsInt();
			else if (name.equals("MaxInvWriteOffAmt"))
				p_MaxInvWriteOffAmt = (BigDecimal)para[i].getParameter();
			else if (name.equals("IsReceipt"))
				p_APAR = (String)para[i].getParameter();
			//
			else if (name.equals("DateTrx"))
			{
				p_DateTrx_From = (Timestamp)para[i].getParameter();
				p_DateTrx_To = (Timestamp)para[i].getParameter_To();
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
			else if (name.equalsIgnoreCase("IsSummary"))
				p_IsSummary = para[i].getParameterAsBoolean();
			else if (name.equalsIgnoreCase("IsPositiveOnly"))
				p_IsPositiveOnly = para[i].getParameterAsBoolean();
			else
				log.log(Level.SEVERE, "Unknown Parameter: " + name);
		
		}
		
		if(p_DateAcct== null) {
			p_DateAcct = new Timestamp(System.currentTimeMillis());
		}
	}
	@Override
	protected String doIt() throws Exception {
		if (log.isLoggable(Level.INFO)) log.info("C_BPartner_ID=" + p_C_BPartner_ID 
				+ ", C_BP_Group_ID=" + p_C_BP_Group_ID
				+ ", C_Payment_ID=" + p_C_Payment_ID
				+ "; IsReceipt=" + p_IsReceipt
				+ ", " + p_DateTrx_From + " - " + p_DateTrx_To
				+ "; CreatePayment=" + p_CreatePayment
				+ ", C_BankAccount_ID=" + p_C_BankAccount_ID);
			//
			StringBuilder sql = new StringBuilder(
				"SELECT C_Payment_ID,DocumentNo,DateTrx,")
				.append(" C_Currency_ID,PayAmt,")
				.append("currencyconvert(paymentavailable(C_Payment_ID),C_Currency_ID,")
				.append(p_C_Currency_ID) // to Currency 
				.append(",DateTrx,c_conversiontype_id ,ad_client_id , ad_org_id ) AS OpenAmt")
				.append(" FROM C_Payment WHERE ");
			
			sql.append(" 1=1 ");
			
			if (p_DateTrx_From != null && p_DateTrx_To != null) {
				sql.append("  AND TRUNC(DateTrx) BETWEEN ")
					.append(DB.TO_DATE(p_DateTrx_From, true))
					.append(" AND ")
					.append(DB.TO_DATE(p_DateTrx_To, true));
			}else if(p_DateTrx_From != null ) {
				
				sql.append("  AND TRUNC(DateTrx) > ")
				.append(DB.TO_DATE(p_DateTrx_From, true)); 
				
			}else if(p_DateTrx_To != null ) {
				
				sql.append("  AND TRUNC(DateTrx) < ")
				.append(DB.TO_DATE(p_DateTrx_To, true)); 
			}
			
			if (p_C_Payment_ID != 0)
				sql.append(" AND C_Payment_ID=").append(p_C_Payment_ID);
			else
			{
				if (p_C_BPartner_ID != 0)
					sql.append(" AND C_BPartner_ID=").append(p_C_BPartner_ID);
				else if (p_C_BP_Group_ID > 0) {
					sql.append(" AND EXISTS (SELECT * FROM C_BPartner bp WHERE C_Payment.C_BPartner_ID=bp.C_BPartner_ID AND bp.C_BP_Group_ID=")
						.append(p_C_BP_Group_ID).append(")");
				}
				//
				
				/*if (p_C_Currency_ID>0)
					sql.append(" AND C_Currency_ID = " + p_C_Currency_ID);*/
				
				
				if (ONLY_AR.equals(p_APAR))
					sql.append(" AND IsReceipt='Y'");
				else if (ONLY_AP.equals(p_APAR))
					sql.append(" AND IsReceipt='N'");
				
				


				sql.append(" AND currencyconvert(paymentavailable(C_Payment_ID),c_currency_id,")
				.append(p_C_Currency_ID) // to Currency 
				.append(",DateTrx,c_conversiontype_id ,ad_client_id , ad_org_id ) <= ")
				.append( p_MaxInvWriteOffAmt );
				
				
			}
			
			//	Added by Jorge Colmenarez, 2021-08-26 08:57
			//	Filter by Payment Not Allocated
			sql.append(" AND IsAllocated = 'N'");
			//	End Jorge Colmenarez
			
			// Added by David Castillo 10/11/2022 
			//Only positive Amt
			if (p_IsPositiveOnly) {
				sql.append(" AND currencyconvert(paymentavailable(C_Payment_ID),C_Currency_ID,")
				.append(p_C_Currency_ID) // to Currency 
				.append(",DateTrx,c_conversiontype_id ,ad_client_id , ad_org_id ) >= 0");
			}
			
			sql.append(" ORDER BY C_Currency_ID, C_BPartner_ID, DateTrx");
			if (log.isLoggable(Level.FINER)) log.finer(sql.toString());
			//
			
			System.out.println(sql);
			int counter = 0;
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			try
			{
				pstmt = DB.prepareStatement (sql.toString(), get_TrxName());
				rs = pstmt.executeQuery ();
				while (rs.next ())
				{
					if (writeOff(rs.getInt(1), rs.getString(2), rs.getTimestamp(3), p_C_Currency_ID , rs.getBigDecimal(6)))
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
			//processPayment();
			createChargeLine(TotalAmt, m_alloc, p_C_Charge_ID,actualC_BPartner_ID);
			processAllocation();
			StringBuilder msgreturn = new StringBuilder("#").append(counter);
			return msgreturn.toString();
		}	//	doIt

		/**
		 * 	Write Off
		 *	@param C_Payment_ID invoice
		 *	@param DocumentNo doc no
		 *	@param DateTrx date
		 *	@param C_Currency_ID currency
		 *	@param OpenAmt open amt
		 *	@return true if written off
		 */
		private boolean writeOff (int C_Payment_ID, String DocumentNo, Timestamp DateTrx, 
			int C_Currency_ID, BigDecimal OpenAmt)
		{
			System.out.println(  "C_Payment_ID = " + C_Payment_ID);
			System.out.println(  "OpenAmt = " + OpenAmt);
			
			//	Nothing to do
			if (OpenAmt == null || OpenAmt.signum() == 0)
				return false;
			if (OpenAmt.abs().compareTo(p_MaxInvWriteOffAmt) >= 0)
				return false;
			//
			if (p_IsSimulation)
			{
				addLog(C_Payment_ID, DateTrx, OpenAmt, DocumentNo);
				return true;
			}
			
			
			//	Invoice
			MPayment Payment = new MPayment(getCtx(), C_Payment_ID, get_TrxName());
			
			//	Allocation
			if (!p_IsSummary) {
				createChargeLine(TotalAmt, m_alloc, p_C_Charge_ID,actualC_BPartner_ID);
				processAllocation();
				m_alloc = new MAllocationHdr (getCtx(), true, 
						p_DateAcct, C_Currency_ID,
						getProcessInfo().getTitle() + " #" + getAD_PInstance_ID(), get_TrxName());
				m_alloc.setAD_Org_ID(Payment.getAD_Org_ID());
				m_alloc.setDateTrx(DateTrx);
				if (!m_alloc.save())
				{
					log.log(Level.SEVERE, "Cannot create allocation header");
					return false;
				}
				
			}else {
				if (m_alloc == null || C_Currency_ID != m_alloc.getC_Currency_ID() || m_alloc.getAD_Org_ID() != Payment.getAD_Org_ID() || actualC_BPartner_ID != Payment.getC_BPartner_ID())
				{
					createChargeLine(TotalAmt, m_alloc, p_C_Charge_ID,actualC_BPartner_ID);
					processAllocation();
					m_alloc = new MAllocationHdr (getCtx(), true, 
							p_DateAcct, C_Currency_ID,
							getProcessInfo().getTitle() + " #" + getAD_PInstance_ID(), get_TrxName());
					m_alloc.setAD_Org_ID(Payment.getAD_Org_ID());
					
					if (!m_alloc.save())
					{
						log.log(Level.SEVERE, "Cannot create allocation header");
						return false;
					}
				}
			}
			

			//	Line
			MAllocationLine aLine = null;
			
			
				aLine = new MAllocationLine (m_alloc,OpenAmt, 
					Env.ZERO, Env.ZERO, Env.ZERO);
			aLine.setC_Payment_ID(Payment.getC_Payment_ID());
			//aLine.setC_Charge_ID(p_C_Charge_ID);
			aLine.setC_BPartner_ID(Payment.getC_BPartner_ID());
			if (aLine.save())
			{
				actualC_BPartner_ID = Payment.getC_BPartner_ID();
				TotalAmt = TotalAmt.add(OpenAmt);
				addLog(C_Payment_ID, DateTrx, OpenAmt, DocumentNo);
				return true;
			}
			//	Error
			log.log(Level.SEVERE, "Cannot create allocation line for C_Payment_ID=" + C_Payment_ID);
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
			//processPayment();
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
		
		private boolean createChargeLine(BigDecimal chargeAmt,MAllocationHdr alloc,int C_Charge_ID,int C_BPartner_ID){
			
			if (alloc != null && chargeAmt.compareTo(Env.ZERO) != 0) {
			MAllocationLine aLine = null;

			//	Modified by Jorge Colmenarez, 2021-08-26 09:11
			//	negate chargeAmt for fix bug when accounting
			aLine = new MAllocationLine (m_alloc, chargeAmt.negate(), 
			Env.ZERO , Env.ZERO , Env.ZERO);
			//	End Jorge Colmenarez
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
