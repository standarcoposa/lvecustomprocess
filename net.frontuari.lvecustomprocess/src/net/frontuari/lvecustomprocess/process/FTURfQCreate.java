package net.frontuari.lvecustomprocess.process;

import java.util.logging.Level;
import org.compiere.model.MRfQ;
import org.compiere.model.MRfQResponse;
import org.compiere.model.MRfQTopic;
import org.compiere.model.MRfQTopicSubscriber;
import org.compiere.process.ProcessInfoParameter;

import net.frontuari.lvecustomprocess.base.FTUProcess;
import net.frontuari.lvecustomprocess.model.FTUMRfQResponse;

public class FTURfQCreate extends FTUProcess{
	/**	Send RfQ				*/
	private boolean	p_IsSendRfQ = false;
	/**	RfQ						*/
	private int		p_C_RfQ_ID = 0;
	
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
			else if (name.equals("IsSendRfQ"))
				p_IsSendRfQ = "Y".equals(para[i].getParameter());
			else
				log.log(Level.SEVERE, "Unknown Parameter: " + name);
		}
		p_C_RfQ_ID = getRecord_ID();
	}	//	prepare

	/**
	 *  Perform process.
	 *  @return Message (translated text)
	 *  @throws Exception if not successful
	 */
	protected String doIt() throws Exception
	{
		MRfQ rfq = new MRfQ (getCtx(), p_C_RfQ_ID, get_TrxName());
		if (log.isLoggable(Level.INFO)) log.info("doIt - " + rfq + ", Send=" + p_IsSendRfQ);
		String error = rfq.checkQuoteTotalAmtOnly();
		if (error != null && error.length() > 0)
			throw new Exception (error);

		int counter = 0;
		int sent = 0;
		int notSent = 0;
		
		//	Get all existing responses
		MRfQResponse[] responses = rfq.getResponses (false, false);
		
		//	Topic
		MRfQTopic topic = new MRfQTopic (getCtx(), rfq.getC_RfQ_Topic_ID(), get_TrxName());
		MRfQTopicSubscriber[] subscribers = topic.getSubscribers();
		for (int i = 0; i < subscribers.length; i++)
		{
			MRfQTopicSubscriber subscriber = subscribers[i];
			boolean skip = false;
			//	existing response
			for (int r = 0; r < responses.length; r++)
			{
				if (subscriber.getC_BPartner_ID() == responses[r].getC_BPartner_ID()
					&& subscriber.getC_BPartner_Location_ID() == responses[r].getC_BPartner_Location_ID())
				{
					skip = true;
					break;
				}
			}
			if (skip)
				continue;
			
			//	Create Response
			FTUMRfQResponse response = new FTUMRfQResponse (rfq, subscriber);
			if (response.get_ID() == 0)	//	no lines
				continue;
			
			counter++;
			if (p_IsSendRfQ)
			{
				if (response.sendRfQ())
					sent++;
				else
					notSent++;
			}
		}	//	for all subscribers

		StringBuilder retValue = new StringBuilder("@Created@ ").append(counter);
		if (p_IsSendRfQ)
			retValue.append(" - @IsSendRfQ@=").append(sent).append(" - @Error@=").append(notSent);
		return retValue.toString();
	}	//	doIt
	
}