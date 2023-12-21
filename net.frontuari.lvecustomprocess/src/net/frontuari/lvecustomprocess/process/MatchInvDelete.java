package net.frontuari.lvecustomprocess.process;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;

import org.adempiere.exceptions.AdempiereException;
import org.compiere.model.MCostDetail;
import org.compiere.model.MMatchInv;
import org.compiere.util.AdempiereUserError;
import org.compiere.util.DB;

import net.frontuari.lvecustomprocess.base.FTUProcess;

/**
 *	Delete Inv Match
 *	
 *  @author Jorg Janke
 *  @version $Id: MatchInvDelete.java,v 1.2 2006/07/30 00:51:01 jjanke Exp $
 *  @author Jorge Colmenarez
 *  @version $Id: MatchInvDelete.java,v 1.3 2022/03/15 14:54:35 jlctmaster Frontuari $
 */
public class MatchInvDelete extends FTUProcess {
	
	/**	ID					*/
	private int		p_M_MatchInv_ID = 0;

	public MatchInvDelete() {
	}

	@Override
	protected void prepare() {
		p_M_MatchInv_ID = getRecord_ID();
	}

	/**
	 * 	Process
	 *	@return message
	 *	@throws Exception
	 */
	@Override
	protected String doIt()	throws Exception
	{
		if (log.isLoggable(Level.INFO)) log.info ("M_MatchInv_ID=" + p_M_MatchInv_ID);
		
		String msg = "";
		
		MMatchInv inv = new MMatchInv (getCtx(), p_M_MatchInv_ID, get_TrxName());
		if (inv.get_ID() == 0)
			throw new AdempiereUserError("@NotFound@ @M_MatchInv_ID@ " + p_M_MatchInv_ID);
		int reversalId = inv.getReversal_ID();
		unProcessCostDetail(inv.get_ID());
		if (!inv.delete(true))
			return "@Error@";
		
		msg += "@Deleted@";
		
		if (reversalId > 0) {
			MMatchInv invrev = new MMatchInv (getCtx(), reversalId, get_TrxName());
			if (invrev.get_ID() == 0)
				throw new AdempiereUserError("@NotFound@ @M_MatchInv_ID@ " + reversalId);
			unProcessCostDetail(invrev.get_ID());
			if (!invrev.delete(true)) {
				return "@Error@ @Reversal_ID@";
			}
			msg += " + @Deleted@ @Reversal_ID@";
		}
		return msg;
	}	//	doIt
	
	/***
	 * Search CostDetail and unprocessed
	 * @param MatchInv_ID
	 */
	private void unProcessCostDetail(int MatchInv_ID)
	{
		String sql = "SELECT M_CostDetail_ID FROM M_CostDetail WHERE M_MatchInv_ID = ? ";
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement (sql, get_TrxName());
			pstmt.setInt (1, MatchInv_ID);
			rs = pstmt.executeQuery ();
			while (rs.next ())
			{
				MCostDetail cd = new MCostDetail (getCtx(), rs.getInt(1), get_TrxName());
				cd.setProcessed(false);
				cd.saveEx(get_TrxName());
			}
		}
		catch (Exception e)
		{
			throw new AdempiereException(e.getLocalizedMessage());
		}
		finally
		{
			DB.close(rs, pstmt);
		}
	}

}
