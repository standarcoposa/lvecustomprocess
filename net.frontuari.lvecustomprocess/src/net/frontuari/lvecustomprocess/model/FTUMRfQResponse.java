package net.frontuari.lvecustomprocess.model;

import java.sql.ResultSet;
import java.util.Properties;
import org.compiere.model.MRfQ;
import org.compiere.model.MRfQLine;
import org.compiere.model.MRfQResponse;
import org.compiere.model.MRfQTopicSubscriber;
import org.compiere.util.Env;

public class FTUMRfQResponse extends MRfQResponse{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1472377321844135042L;


	/**
	 * 	Standard Constructor
	 *	@param ctx context
	 *	@param C_RfQResponse_ID id
	 *	@param trxName transaction
	 */
	public FTUMRfQResponse (Properties ctx, int C_RfQResponse_ID, String trxName)
	{
		super (ctx, C_RfQResponse_ID, trxName);
		if (C_RfQResponse_ID == 0)
		{
			setIsComplete (false);
			setIsSelectedWinner (false);
			setIsSelfService (false);
			setPrice (Env.ZERO);
			setProcessed(false);
			setProcessing(false);
		}
	}	//	MRfQResponse
	
	public FTUMRfQResponse (Properties ctx, ResultSet rs, String trxName)
	{
		super(ctx, rs, trxName);
	}	//	MRfQResponse

	
	public FTUMRfQResponse (MRfQ rfq, MRfQTopicSubscriber subscriber)
	{
		this (rfq, subscriber, 
			subscriber.getC_BPartner_ID(), 
			subscriber.getC_BPartner_Location_ID(), 
			subscriber.getAD_User_ID());
	}	//	MRfQResponse

	public FTUMRfQResponse (MRfQ rfq, MRfQTopicSubscriber subscriber,
		int C_BPartner_ID, int C_BPartner_Location_ID, int AD_User_ID)
	{
		
		this (rfq.getCtx(), 0, rfq.get_TrxName());
		setClientOrg(rfq);
		setC_RfQ_ID(rfq.getC_RfQ_ID());
		setC_Currency_ID (rfq.getC_Currency_ID());
		setName (rfq.getName());
		setDescription(rfq.getDescription());
		//	Subscriber info
		setC_BPartner_ID (C_BPartner_ID);
		setC_BPartner_Location_ID (C_BPartner_Location_ID);
		setAD_User_ID(AD_User_ID);
		
		//	Create Lines
		MRfQLine[] lines = rfq.getLines();
		for (int i = 0; i < lines.length; i++)
		{
			if (!lines[i].isActive())
				continue;
			
			//	Product on "Only" list
			if (subscriber != null 
				&& !subscriber.isIncluded(lines[i].getM_Product_ID() ))
				continue;
			//
			if (get_ID() == 0)	//	save Response
				saveEx();

			@SuppressWarnings("unused")
			FTUMRfQResponseLine line = new FTUMRfQResponseLine (this, lines[i]);
			//	line is not saved (dumped) if there are no Qtys 
		}
	}	//	MRfQResponse
}