package net.frontuari.lvecustomprocess.model;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.logging.Level;

import org.compiere.model.MAllocationHdr;
import org.compiere.model.MAllocationLine;
import org.compiere.model.MBPartner;
import org.compiere.model.MClient;
import org.compiere.model.MConversionRate;
import org.compiere.model.MConversionRateUtil;
import org.compiere.model.MCurrency;
import org.compiere.model.MDocType;
import org.compiere.model.MFactAcct;
import org.compiere.model.MInvoice;
import org.compiere.model.MPayment;
import org.compiere.model.MPeriod;
import org.compiere.util.DB;
import org.compiere.util.Env;

public class MFTUAllocationHdr extends MAllocationHdr {

	/**
	 * 
	 */
	private static final long serialVersionUID = -203212341272428783L;

	public MFTUAllocationHdr(Properties ctx, int C_AllocationHdr_ID, String trxName) {
		super(ctx, C_AllocationHdr_ID, trxName);
	}

	public MFTUAllocationHdr(Properties ctx, boolean IsManual, Timestamp DateTrx, int C_Currency_ID, String description,
			String trxName) {
		super(ctx, IsManual, DateTrx, C_Currency_ID, description, trxName);
	}

	public MFTUAllocationHdr(Properties ctx, ResultSet rs, String trxName) {
		super(ctx, rs, trxName);
	}
	
	/**	Lines						*/
	private MAllocationLine[]	m_lines = null;
	/**	Process Message 			*/
	private String		m_processMsg = null;
	/**	Tolerance Gain and Loss */
	private static final BigDecimal	TOLERANCE = BigDecimal.valueOf(0.02);
	
	/**
	 * 	Before Delete.
	 *	@return true if acct was deleted
	 */
	@Override
	protected boolean beforeDelete ()
	{
		String trxName = get_TrxName();
		if (trxName == null || trxName.length() == 0)
			log.warning ("No transaction");
		if (isPosted())
		{
			//	Modified by Jorge Colmenarez, 2022-12-12 15:24
			//	Change DateTrx by DateAcct
			//	MPeriod.testPeriodOpen(getCtx(), getDateTrx(), MDocType.DOCBASETYPE_PaymentAllocation, getAD_Org_ID());
			MPeriod.testPeriodOpen(getCtx(), getDateAcct(), MDocType.DOCBASETYPE_PaymentAllocation, getAD_Org_ID());
			setPosted(false);
			MFactAcct.deleteEx (Table_ID, get_ID(), trxName);
			//	End Jorge Colmenarez
		}
		//	Mark as Inactive
		setIsActive(false);		//	updated DB for line delete/process
		this.saveEx();

		//	Unlink
		getLines(true);
		if (!updateBP(true)) 
			return false;
		
		for (int i = 0; i < m_lines.length; i++)
		{
			MAllocationLine line = m_lines[i];
			line.deleteEx(true, trxName);
		}
		return true;
	}	//	beforeDelete
	
	private boolean updateBP(boolean reverse)
	{
		
		m_lines = getLines(false);
		for (MAllocationLine line : m_lines) {
			int C_Payment_ID = line.getC_Payment_ID();
			int C_BPartner_ID = line.getC_BPartner_ID();
			int M_Invoice_ID = line.getC_Invoice_ID();

			if ((C_BPartner_ID == 0) || ((M_Invoice_ID == 0) && (C_Payment_ID == 0)))
				continue;

			boolean isSOTrxInvoice = false;
			MInvoice invoice = M_Invoice_ID > 0 ? new MInvoice (getCtx(), M_Invoice_ID, get_TrxName()) : null;
			if (M_Invoice_ID > 0)
				isSOTrxInvoice = invoice.isSOTrx();
			
			MBPartner bpartner = new MBPartner (getCtx(), line.getC_BPartner_ID(), get_TrxName());
			DB.getDatabase().forUpdate(bpartner, 0);

			BigDecimal allocAmt = line.getAmount().add(line.getDiscountAmt()).add(line.getWriteOffAmt());
			BigDecimal openBalanceDiff = Env.ZERO;
			MClient client = MClient.get(getCtx(), getAD_Client_ID());
			
			boolean paymentProcessed = false;
			boolean paymentIsReceipt = false;
			
			// Retrieve payment information
			if (C_Payment_ID > 0)
			{
				MPayment payment = null;
				int convTypeID = 0;
				Timestamp paymentDate = null;
				
				payment = new MPayment (getCtx(), C_Payment_ID, get_TrxName());
				convTypeID = payment.getC_ConversionType_ID();
				paymentDate = payment.getDateAcct();
				paymentProcessed = payment.isProcessed();
				paymentIsReceipt = payment.isReceipt();
						
				// Adjust open amount with allocated amount. 
				if (paymentProcessed)
				{
					if (invoice != null)
					{
						// If payment is already processed, only adjust open balance by discount and write off amounts.
						BigDecimal amt = MConversionRate.convertBase(getCtx(), line.getWriteOffAmt().add(line.getDiscountAmt()),
								getC_Currency_ID(), paymentDate, convTypeID, getAD_Client_ID(), getAD_Org_ID());
						if (amt == null)
						{
							m_processMsg = MConversionRateUtil.getErrorMessage(getCtx(), "ErrorConvertingAllocationCurrencyToBaseCurrency",
									getC_Currency_ID(), MClient.get(getCtx()).getC_Currency_ID(), convTypeID, paymentDate, get_TrxName());
							return false;
						}
						openBalanceDiff = openBalanceDiff.add(amt);
					}
					else
					{
						// Allocating payment to payment.
						BigDecimal amt = MConversionRate.convertBase(getCtx(), allocAmt,
								getC_Currency_ID(), paymentDate, convTypeID, getAD_Client_ID(), getAD_Org_ID());
						if (amt == null)
						{
							m_processMsg = MConversionRateUtil.getErrorMessage(getCtx(), "ErrorConvertingAllocationCurrencyToBaseCurrency",
									getC_Currency_ID(), MClient.get(getCtx()).getC_Currency_ID(), convTypeID, paymentDate, get_TrxName());
							return false;
						}
						openBalanceDiff = openBalanceDiff.add(amt);
					}
				} else {
					// If payment has not been processed, adjust open balance by entire allocated amount.
					BigDecimal allocAmtBase = MConversionRate.convertBase(getCtx(), allocAmt,	
							getC_Currency_ID(), getDateAcct(), convTypeID, getAD_Client_ID(), getAD_Org_ID());
					if (allocAmtBase == null)
					{
						m_processMsg = MConversionRateUtil.getErrorMessage(getCtx(), "ErrorConvertingAllocationCurrencyToBaseCurrency",
								getC_Currency_ID(), MClient.get(getCtx()).getC_Currency_ID(), convTypeID, getDateAcct(), get_TrxName());
						return false;
					}
	
					openBalanceDiff = openBalanceDiff.add(allocAmtBase);
				}
			}
			else if (invoice != null)
			{
				// adjust open balance by discount and write off amounts.
				BigDecimal amt = MConversionRate.convertBase(getCtx(), allocAmt.negate(),
						getC_Currency_ID(), invoice.getDateAcct(), invoice.getC_ConversionType_ID(), getAD_Client_ID(), getAD_Org_ID());
				if (amt == null)
				{
					m_processMsg = MConversionRateUtil.getErrorMessage(getCtx(), "ErrorConvertingAllocationCurrencyToBaseCurrency",
							getC_Currency_ID(), MClient.get(getCtx()).getC_Currency_ID(), invoice.getC_ConversionType_ID(), invoice.getDateAcct(), get_TrxName());
					return false;
				}
				openBalanceDiff = openBalanceDiff.add(amt);
			}
			
			// Adjust open amount for currency gain/loss
			if ((invoice != null) && 
					((getC_Currency_ID() != client.getC_Currency_ID()) ||
					 (getC_Currency_ID() != invoice.getC_Currency_ID())))
			{
				if (getC_Currency_ID() != invoice.getC_Currency_ID())
				{
					allocAmt = MConversionRate.convert(getCtx(), allocAmt,	
							getC_Currency_ID(), invoice.getC_Currency_ID(), getDateAcct(), invoice.getC_ConversionType_ID(), getAD_Client_ID(), getAD_Org_ID());
					if (allocAmt == null)
					{
						m_processMsg = MConversionRateUtil.getErrorMessage(getCtx(), "ErrorConvertingAllocationCurrencyToInvoiceCurrency",
								getC_Currency_ID(), invoice.getC_Currency_ID(), invoice.getC_ConversionType_ID(), getDateAcct(), get_TrxName());
						return false;
					}
				}
				BigDecimal invAmtAccted = MConversionRate.convertBase(getCtx(), invoice.getGrandTotal(),	
						invoice.getC_Currency_ID(), invoice.getDateAcct(), invoice.getC_ConversionType_ID(), getAD_Client_ID(), getAD_Org_ID());
				if (invAmtAccted == null)
				{
					m_processMsg = MConversionRateUtil.getErrorMessage(getCtx(), "ErrorConvertingInvoiceCurrencyToBaseCurrency",
							invoice.getC_Currency_ID(), MClient.get(getCtx()).getC_Currency_ID(), invoice.getC_ConversionType_ID(), invoice.getDateAcct(), get_TrxName());
					return false;
				}
				
				BigDecimal allocAmtAccted = MConversionRate.convertBase(getCtx(), allocAmt,	
						invoice.getC_Currency_ID(), getDateAcct(), invoice.getC_ConversionType_ID(), getAD_Client_ID(), getAD_Org_ID());
				if (allocAmtAccted == null)
				{
					m_processMsg = MConversionRateUtil.getErrorMessage(getCtx(), "ErrorConvertingInvoiceCurrencyToBaseCurrency",
							invoice.getC_Currency_ID(), MClient.get(getCtx()).getC_Currency_ID(), invoice.getC_ConversionType_ID(), getDateAcct(), get_TrxName());
					return false;
				}

				if (allocAmt.compareTo(invoice.getGrandTotal()) == 0)
				{
					openBalanceDiff = openBalanceDiff.add(invAmtAccted).subtract(allocAmtAccted);
				}
				else
				{
					//	allocation as a percentage of the invoice
					double multiplier = allocAmt.doubleValue() / invoice.getGrandTotal().doubleValue();
					//	Reduce Orig Invoice Accounted
					invAmtAccted = invAmtAccted.multiply(BigDecimal.valueOf(multiplier));
					//	Difference based on percentage of Orig Invoice
					openBalanceDiff = openBalanceDiff.add(invAmtAccted).subtract(allocAmtAccted);	//	gain is negative
					//	ignore Tolerance
					if (openBalanceDiff.abs().compareTo(TOLERANCE) < 0)
						openBalanceDiff = Env.ZERO;
					//	Round
					int precision = MCurrency.getStdPrecision(getCtx(), client.getC_Currency_ID());
					if (openBalanceDiff.scale() > precision)
						openBalanceDiff = openBalanceDiff.setScale(precision, RoundingMode.HALF_UP);
				}
			}			
			
			//	Total Balance
			BigDecimal newBalance = bpartner.getTotalOpenBalance();
			if (newBalance == null)
				newBalance = Env.ZERO;
			
			BigDecimal originalBalance = new BigDecimal(newBalance.toString());

			if (openBalanceDiff.signum() != 0)
			{
				if (reverse)
					newBalance = newBalance.add(openBalanceDiff);
				else
					newBalance = newBalance.subtract(openBalanceDiff);
			}

			// Update BP Credit Used only for Customer Invoices and for payment-to-payment allocations.
			BigDecimal newCreditAmt = Env.ZERO;
			if (isSOTrxInvoice || (invoice == null && paymentIsReceipt && paymentProcessed))
			{
				if (invoice == null)
					openBalanceDiff = openBalanceDiff.negate();

				newCreditAmt = bpartner.getSO_CreditUsed();

				if(reverse)
				{
					if (newCreditAmt == null)
						newCreditAmt = openBalanceDiff;
					else
						newCreditAmt = newCreditAmt.add(openBalanceDiff);
				}
				else
				{
					if (newCreditAmt == null)
						newCreditAmt = openBalanceDiff.negate();
					else
						newCreditAmt = newCreditAmt.subtract(openBalanceDiff);
				}

				if (log.isLoggable(Level.FINE))
				{
					log.fine("TotalOpenBalance=" + bpartner.getTotalOpenBalance() + "(" + openBalanceDiff
							+ ", Credit=" + bpartner.getSO_CreditUsed() + "->" + newCreditAmt
							+ ", Balance=" + bpartner.getTotalOpenBalance() + " -> " + newBalance);
				}
				bpartner.setSO_CreditUsed(newCreditAmt);
			}
			else
			{
				if (log.isLoggable(Level.FINE))
				{
					log.fine("TotalOpenBalance=" + bpartner.getTotalOpenBalance() + "(" + openBalanceDiff
							+ ", Balance=" + bpartner.getTotalOpenBalance() + " -> " + newBalance);				
				}
			}

			if (newBalance.compareTo(originalBalance) != 0)
				bpartner.setTotalOpenBalance(newBalance);
			
			bpartner.setSOCreditStatus();
			if (!bpartner.save(get_TrxName()))
			{
				m_processMsg = "Could not update Business Partner";
				return false;
			}

		} // for all lines

		return true;
	}	//	updateBP

}
