package net.frontuari.lvecustomprocess.process;

import java.sql.Timestamp;
import java.util.logging.Level;

import org.compiere.model.MBPartner;
import org.compiere.model.MCommission;
import org.compiere.model.MCommissionRun;
import org.compiere.model.MDocType;
import org.compiere.model.MInvoice;
import org.compiere.model.MInvoiceLine;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.util.Env;

import net.frontuari.lvecustomprocess.base.FTUProcess;

public class FTUCommissionAPInvoice extends FTUProcess{

	Timestamp p_DateInvoiced = new Timestamp(System.currentTimeMillis());
	int p_C_DocType_ID = 0;
	boolean p_IsSOTrx = false;
	protected void prepare()
	{
		ProcessInfoParameter[] para = getParameter();
		for (int i = 0; i < para.length; i++)
		{
			String name = para[i].getParameterName();
			if (para[i].getParameter() == null)
				;
			else if (name.equalsIgnoreCase("DateInvoiced")) {
				p_DateInvoiced = para[i].getParameterAsTimestamp();
			}
			else if (name.equalsIgnoreCase("C_DocType_ID")) {
				p_C_DocType_ID = para[i].getParameterAsInt();
			}
			else if (name.equalsIgnoreCase("IsSOTrx")) {
				p_IsSOTrx = para[i].getParameterAsBoolean();
			}
			else
				log.log(Level.SEVERE, "prepare - Unknown Parameter: " + name);
		}
	}	//	prepare

	/**
	 *  Perform process.
	 *  @return Message (variables are parsed)
	 *  @throws Exception if not successful
	 */
	protected String doIt() throws Exception
	{
		if (log.isLoggable(Level.INFO)) log.info("doIt - C_CommissionRun_ID=" + getRecord_ID());
		//	Load Data
		MCommissionRun comRun = new MCommissionRun (getCtx(), getRecord_ID(), get_TrxName());
		if (comRun.get_ID() == 0)
			throw new IllegalArgumentException("CommissionAPInvoice - No Commission Run");
		if (Env.ZERO.compareTo(comRun.getGrandTotal()) == 0)
			throw new IllegalArgumentException("@GrandTotal@ = 0");
		MCommission com = new MCommission (getCtx(), comRun.getC_Commission_ID(), get_TrxName());
		if (com.get_ID() == 0)
			throw new IllegalArgumentException("CommissionAPInvoice - No Commission");
		if (com.getC_Charge_ID() == 0)
			throw new IllegalArgumentException("CommissionAPInvoice - No Charge on Commission");
		MBPartner bp = new MBPartner (getCtx(), com.getC_BPartner_ID(), get_TrxName());
		if (bp.get_ID() == 0)
			throw new IllegalArgumentException("CommissionAPInvoice - No BPartner");
			
		//	Create Invoice
		MInvoice invoice = new MInvoice (getCtx(), 0, null);
		invoice.setClientOrg(com.getAD_Client_ID(), com.getAD_Org_ID());
		invoice.setC_DocTypeTarget_ID(p_C_DocType_ID);	//	API
		invoice.setBPartner(bp);
	//	invoice.setDocumentNo (comRun.getDocumentNo());		//	may cause unique constraint
		invoice.setSalesRep_ID(getAD_User_ID());	//	caller
		invoice.setC_Currency_ID(com.getC_Currency_ID());
		invoice.setDateInvoiced(p_DateInvoiced);
		invoice.setDateAcct(p_DateInvoiced);
		invoice.setIsSOTrx(p_IsSOTrx);
		//
		/*if (com.getC_Currency_ID() != invoice.getC_Currency_ID())
			throw new IllegalArgumentException("CommissionAPInvoice - Currency of PO Price List not Commission Currency");*/
		//		
		if (!invoice.save())
			throw new IllegalStateException("CommissionAPInvoice - cannot save Invoice");
			
 		//	Create Invoice Line
 		MInvoiceLine iLine = new MInvoiceLine(invoice);
		iLine.setC_Charge_ID(com.getC_Charge_ID());
 		iLine.setQty(1);
 		iLine.setPrice(comRun.getGrandTotal());
		iLine.setTax();
		if (!iLine.save())
			throw new IllegalStateException("CommissionAPInvoice - cannot save Invoice Line");
		//
		return "@C_Invoice_ID@ = " + invoice.getDocumentNo();
	}	//	doIt

}	//	CommissionAPInvoice


