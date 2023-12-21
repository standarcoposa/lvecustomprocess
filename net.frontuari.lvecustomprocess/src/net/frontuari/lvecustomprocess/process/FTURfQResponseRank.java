package net.frontuari.lvecustomprocess.process;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;

import org.adempiere.exceptions.AdempiereException;
import org.compiere.model.MRfQ;
import org.compiere.model.MRfQLine;
import org.compiere.model.MRfQLineQty;
import org.compiere.model.MRfQResponse;
import org.compiere.model.MRfQResponseLine;
import org.compiere.model.MRfQResponseLineQty;
import org.compiere.model.Query;
import org.compiere.util.Env;

import net.frontuari.lvecustomprocess.base.FTUProcess;

/**
 * 
 * @author Argenis Rodríguez argenismrvel@gmail.com
 *
 */
public class FTURfQResponseRank extends FTUProcess {
	
	private static String QUOTEBY_MinimalPrice	= "MP";
	
	@Override
	protected void prepare() {
		
	}
	
	@Override
	protected String doIt() throws Exception {
		
		if (getRecord_ID() <= 0)
			throw new AdempiereException("@No@ @C_RfQ_ID@");
		
		MRfQ rfq = new MRfQ(getCtx(), getRecord_ID(), get_TrxName());
		String error = Optional.ofNullable(rfq.checkQuoteTotalAmtOnly()).orElse("");
		
		if (!error.isEmpty())
			throw new AdempiereException(error);
		
		MRfQResponse [] responses = rfq.getResponses(true, true);
		
		if (responses.length == 0)
			throw new IllegalArgumentException("No completed RfQ Responses found");
		
		if (responses.length == 1)
		{
			responses[0].setIsSelectedWinner(true);
			responses[0].saveEx();
			return "Only one completed RfQ Response found";
		}
		
		if (rfq.isQuoteTotalAmt())
			rankResponses(rfq, responses);
		else if (rfq.isQuoteSelectedLines())
			rankLines(rfq, responses);
		else
			rankAllLines(rfq, responses);
		StringBuilder msgreturn = new StringBuilder("# ").append(responses.length);
		return msgreturn.toString();
	}
	
	private void rankLines(MRfQ rfq, MRfQResponse [] responses) {
		
		MRfQLine [] lines = rfq.getLines();
		
		if (lines.length == 0)
			throw new IllegalArgumentException("No RfQ Lines found");
		
		for (MRfQLine line: lines)
			for (MRfQLineQty lineQty: line.getQtys(false))
			{
				if (!lineQty.isRfQQty())
					continue;
				
				BigDecimal qty = lineQty.getQty();
				MRfQResponseLineQty [] responseQty = getResponseQty(lineQty, rfq.get_ValueAsString("QuoteBy"));
				int rank = 1;
				int lastRank = rank;
				BigDecimal lastAmt = BigDecimal.ZERO;
				BigDecimal lastQty = BigDecimal.ZERO;
				
				for (MRfQResponseLineQty responseLineQty: responseQty)
				{
					
					BigDecimal proposedQty = Optional.ofNullable((BigDecimal) responseLineQty.get_Value("ProposedQuantity"))
							.orElse(BigDecimal.ZERO);
					
					if (!responseLineQty.isValidAmt() || proposedQty.compareTo(BigDecimal.ZERO) == 0)
					{
						responseLineQty.setRanking(999);
						responseLineQty.saveEx();
						continue;
					}
					
					BigDecimal netAmt = responseLineQty.getNetAmt();
					
					BigDecimal quotationQty = proposedQty.compareTo(qty) > 0 ? qty : proposedQty;
					
					if (BigDecimal.ZERO.compareTo(qty) == 0
							&& lastAmt.compareTo(netAmt) != 0
							&& lastQty.compareTo(proposedQty) != 0)
					{
						lastAmt = netAmt;
						lastRank = rank;
						lastQty = proposedQty;
					}
					
					qty = qty.subtract(quotationQty);
					
					responseLineQty.setRanking(lastRank);
					responseLineQty.set_ValueOfColumn("QuotationQuantity", quotationQty);
					responseLineQty.saveEx();
					
					if (rank == 1)
					{
						lineQty.setBestResponseAmt(netAmt);
						lineQty.saveEx();
					}
					rank++;
				}
			}
		//MRfQResponse winner = null;
		for (MRfQResponse response: responses)
		{
			if (response.isSelectedWinner())
				response.setIsSelectedWinner(false);
			int ranking = 0;
			
			for (MRfQResponseLine responseLine: response.getLines())
			{
				if (responseLine.isSelectedWinner())
				{
					responseLine.setIsSelectedWinner(false);
					responseLine.saveEx();
				}
				
				for (MRfQResponseLineQty responseLineQty: responseLine.getQtys())
				{
					ranking += responseLineQty.getRanking();
					BigDecimal quotationQty = Optional.ofNullable((BigDecimal) responseLineQty.get_Value("QuotationQuantity"))
							.orElse(BigDecimal.ZERO);
					
					if (responseLineQty.getRanking() == 1
							&& quotationQty.compareTo(BigDecimal.ZERO) > 0
							&& responseLineQty.getRfQLineQty().isRfQQty())
					{
						responseLine.setIsSelectedWinner(true);
						responseLine.saveEx();
						break;
					}
				}
			}
			
			response.setRanking(ranking);
			response.saveEx();
			
			/*if (!rfq.isQuoteSelectedLines())
			{
				if (winner == null && ranking > 0)
					winner = response;
				if (winner != null 
						&& response.getRanking() > 0 
						&& response.getRanking() < winner.getRanking())
					winner = response;
			}*/
		}
	}
	
	private void rankAllLines(MRfQ rfq, MRfQResponse[] responses)
	{
		MRfQLine[] rfqLines = rfq.getLines();
		if (rfqLines.length == 0)
			throw new IllegalArgumentException("No RfQ Lines found");
		
		//	 for all lines
		for (int i = 0; i < rfqLines.length; i++)
		{
			//	RfQ Line
			MRfQLine rfqLine = rfqLines[i];
			if (!rfqLine.isActive())
				continue;
			if (log.isLoggable(Level.FINE)) log.fine("rankLines - " + rfqLine);
			MRfQLineQty[] rfqQtys = rfqLine.getQtys();
			for (int j = 0; j < rfqQtys.length; j++)
			{
				//	RfQ Line Qty
				MRfQLineQty rfqQty = rfqQtys[j];
				if (!rfqQty.isActive() || !rfqQty.isRfQQty())
					continue;
				if (log.isLoggable(Level.FINE)) log.fine("rankLines Qty - " + rfqQty);
				MRfQResponseLineQty[] respQtys = rfqQty.getResponseQtys(false);
				for (int kk = 0; kk < respQtys.length; kk++)
				{
					//	Response Line Qty
					MRfQResponseLineQty respQty = respQtys[kk];
					if (!respQty.isActive() || !respQty.isValidAmt())
					{
						respQty.setRanking(999);
						respQty.saveEx();
						if (log.isLoggable(Level.FINE)) log.fine("  - ignored: " + respQty);
					}
				}	//	for all respones line qtys
				
				//	Rank RfQ Line Qtys
				respQtys = rfqQty.getResponseQtys(false);
				if (respQtys.length == 0) {
					if (log.isLoggable(Level.FINE)) log.fine("  - No Qtys with valid Amounts");
				} else {
					Arrays.sort(respQtys, respQtys[0]);
					int lastRank = 1;		//	multiple rank #1
					BigDecimal lastAmt = Env.ZERO;
					int rank = 0;
					for (int k = 0; k < respQtys.length; k++)
					{
						MRfQResponseLineQty qty = respQtys[k];
						if (!qty.isActive() || qty.getRanking() == 999)
						{
							continue;
						}
						BigDecimal netAmt = qty.getNetAmt();
						if (netAmt == null)
						{
							qty.setRanking(999);
							qty.saveEx();
							if (log.isLoggable(Level.FINE)) log.fine("  - Rank 999: " + qty);
							continue;
						}
						
						if (lastAmt.compareTo(netAmt) != 0)
						{
							lastRank = rank+1;
							lastAmt = qty.getNetAmt();
						}
						qty.setRanking(lastRank);
						if (log.isLoggable(Level.FINE)) log.fine("  - Rank " + lastRank + ": " + qty);
						qty.saveEx();
						//	
						if (rank == 0)	//	Update RfQ
						{
							rfqQty.setBestResponseAmt(qty.getNetAmt());
							rfqQty.saveEx();
						}
						rank++;
					}
				}
			}	//	for all rfq line qtys
		}	//	 for all rfq lines
		
		//	Select Winner based on line ranking
		MRfQResponse winner = null;
		for (int ii = 0; ii < responses.length; ii++)
		{
			MRfQResponse response = responses[ii];
			if (response.isSelectedWinner())
				response.setIsSelectedWinner(false);
			int ranking = 0;
			MRfQResponseLine[] respLines = response.getLines(false);
			for (int jj = 0; jj < respLines.length; jj++)
			{
				//	Response Line
				MRfQResponseLine respLine = respLines[jj];
				if (!respLine.isActive())
					continue;
				if (respLine.isSelectedWinner())
				{
					respLine.setIsSelectedWinner(false);
					respLine.saveEx();
				}
				MRfQResponseLineQty[] respQtys = respLine.getQtys(false);
				for (int kk = 0; kk < respQtys.length; kk++)
				{
					//	Response Line Qty
					MRfQResponseLineQty respQty = respQtys[kk];
					if (!respQty.isActive())
						continue;
					ranking += respQty.getRanking();
					if (respQty.getRanking() == 1 
						&& respQty.getRfQLineQty().isPurchaseQty())
					{
						respLine.setIsSelectedWinner(true);
						respLine.saveEx();
						break;
					}
				}
			}
			response.setRanking(ranking);
			response.saveEx();
			if (log.isLoggable(Level.FINE)) log.fine("- Response Ranking " + ranking + ": " + response);
			
			if (winner == null && ranking > 0)
				winner = response;
			else if (winner != null 
					&& response.getRanking() > 0 
					&& response.getRanking() < winner.getRanking())
				winner = response;
		}
		if (winner != null)
		{
			winner.setIsSelectedWinner(true);
			winner.saveEx();
			if (log.isLoggable(Level.FINE)) log.fine("- Response Winner: " + winner);
		}
	}	//	rankLines
	
	private MRfQResponseLineQty [] getResponseQty(MRfQLineQty line, String quoteBy) {
		
		String where	= MRfQLineQty.COLUMNNAME_C_RfQLineQty_ID + "=?";
		String orderBy	= null;
		
		if (QUOTEBY_MinimalPrice.equals(quoteBy))
			orderBy = MRfQResponseLineQty.COLUMNNAME_Price + ", ProposedQuantity DESC";
		else
			orderBy = "ProposedQuantity DESC, " + MRfQResponseLineQty.COLUMNNAME_Price;
		
		List<MRfQResponseLineQty> responses = new Query(getCtx(), MRfQResponseLineQty.Table_Name, where.toString(), get_TrxName())
				.setOnlyActiveRecords(true)
				.setParameters(line.get_ID())
				.setOrderBy(orderBy)
				.list();
		
		return responses.toArray(new MRfQResponseLineQty[ responses.size() ]);
	}
	
	/**************************************************************************
	 * 	Rank Response based on Header
	 *	@param rfq RfQ
	 *	@param responses responses
	 */
	private void rankResponses (MRfQ rfq, MRfQResponse[] responses)
	{
		int ranking = 1;
		//	Responses Ordered by Price
		for (int ii = 0; ii < responses.length; ii++)
		{
			MRfQResponse response = responses[ii];
			if (response.getPrice() != null 
				&& response.getPrice().compareTo(Env.ZERO) > 0)
			{
				if (response.isSelectedWinner() != (ranking == 1))
					response.setIsSelectedWinner(ranking == 1);
				response.setRanking(ranking);
				//
				ranking++;
			}
			else
			{
				response.setRanking(999);
				if (response.isSelectedWinner())
					response.setIsSelectedWinner(false);
			}
			response.saveEx();
			if (log.isLoggable(Level.FINE)) log.fine("rankResponse - " + response);
		}
	}	//	rankResponses

}
