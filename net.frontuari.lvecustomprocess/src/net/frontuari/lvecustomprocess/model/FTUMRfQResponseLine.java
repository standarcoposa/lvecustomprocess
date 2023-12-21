package net.frontuari.lvecustomprocess.model;

import java.sql.ResultSet;
import java.util.Properties;

import org.compiere.model.MRfQLine;
import org.compiere.model.MRfQLineQty;
import org.compiere.model.MRfQResponse;
import org.compiere.model.MRfQResponseLine;
import org.compiere.model.MRfQResponseLineQty;

public class FTUMRfQResponseLine extends MRfQResponseLine{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public FTUMRfQResponseLine (Properties ctx, int C_RfQResponseLine_ID, String trxName)
	{
		super(ctx, C_RfQResponseLine_ID, trxName);
//		if (ignored != 0)
//			throw new IllegalArgumentException("Multi-Key");
	}	//	MRfQResponseLine

	/**
	 * 	Load Constructor
	 *	@param ctx context
	 *	@param rs result set
	 *	@param trxName transaction
	 */
	public FTUMRfQResponseLine (Properties ctx, ResultSet rs, String trxName)
	{
		super(ctx, rs, trxName);
	}	//	MRfQResponseLine
	
	/**
	 * 	Parent Constructor.
	 * 	Also creates qtys if RfQ Qty
	 * 	Is saved if there are qtys(!)
	 *	@param response response
	 *	@param line line
	 */
	public FTUMRfQResponseLine (MRfQResponse response, MRfQLine line)
	{
		super (response.getCtx(), 0, response.get_TrxName());
		setClientOrg(response);
		setC_RfQResponse_ID (response.getC_RfQResponse_ID());
		//
		setC_RfQLine_ID (line.getC_RfQLine_ID());
		setDescription(line.getDescription());
		//
		setIsSelectedWinner (false);
		setIsSelfService (false);
		//
		MRfQLineQty[] qtys = line.getQtys();
		for (int i = 0; i < qtys.length; i++)
		{
			if (qtys[i].isActive() && qtys[i].isRfQQty())
			{
				if (get_ID() == 0)	//	save this line
					saveEx();
				MRfQResponseLineQty qty = new MRfQResponseLineQty (this, qtys[i]);
				qty.saveEx();
			}
		}
	}
}
