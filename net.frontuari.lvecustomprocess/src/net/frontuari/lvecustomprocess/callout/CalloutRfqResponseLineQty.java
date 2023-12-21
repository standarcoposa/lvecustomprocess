package net.frontuari.lvecustomprocess.callout;

import java.util.Optional;

import org.compiere.model.MRfQ;
import org.compiere.model.MRfQLine;
import org.compiere.model.MRfQLineQty;
import org.compiere.model.MRfQResponseLineQty;
import org.compiere.util.Env;

import net.frontuari.lvecustomprocess.base.FTUCallout;

public class CalloutRfqResponseLineQty extends FTUCallout {

	@Override
	protected String start() {
		
		if (!MRfQResponseLineQty.COLUMNNAME_C_RfQResponseLineQty_ID.equals(getColumnName()))
			return null;
		
		int C_RfQLineQty_ID = Optional.ofNullable((Integer) getTab().getValue(MRfQResponseLineQty.COLUMNNAME_C_RfQLineQty_ID))
				.orElse(0);
		
		MRfQLineQty lineQty = new MRfQLineQty(getCtx(), C_RfQLineQty_ID, null);
		
		if (lineQty.get_ID() == 0)
			return null;
		
		MRfQLine line = (MRfQLine) lineQty.getC_RfQLine();
		MRfQ rfq = (MRfQ) line.getC_RfQ();
		
		Env.setContext(getCtx(), getWindowNo(), MRfQ.COLUMNNAME_QuoteType, rfq.getQuoteType());
		
		return null;
	}

}
