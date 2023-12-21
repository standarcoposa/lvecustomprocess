package net.frontuari.lvecustomprocess.process;

import java.math.BigDecimal;
import java.util.List;
import java.util.logging.Level;

import org.compiere.model.MBPartner;
import org.compiere.model.MOrder;
import org.compiere.model.MOrderLine;
import org.compiere.model.MRequisition;
import org.compiere.model.MRequisitionLine;
import org.compiere.model.MRfQ;
import org.compiere.model.MRfQLine;
import org.compiere.model.MRfQResponse;
import org.compiere.model.MRfQResponseLineQty;
import org.compiere.model.Query;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.util.DB;

import net.frontuari.lvecustomprocess.base.FTUProcess;
import net.frontuari.lvecustomprocess.model.FTUMRfQResponse;
import net.frontuari.lvecustomprocess.model.FTUMRfQResponseLine;

public class FTURfQCreatePO extends FTUProcess{
	/**	RfQ 			*/
	private int		p_C_RfQ_ID = 0;
	private int		p_C_DocType_ID = 0;
	private boolean p_CloseIt = false;
	/**
	 * 	Prepare
	 */
	protected void prepare ()
	{
		ProcessInfoParameter[] para = getParameter();
		for (int i = 0; i < para.length; i++)
		{
			String name = para[i].getParameterName();
			if (para[i].getParameter() == null)
				;
			else if (name.equals("C_DocType_ID"))
				p_C_DocType_ID = para[i].getParameterAsInt();
			else if (name.equals("CloseIt"))
				p_CloseIt = para[i].getParameterAsBoolean();
			else
				log.log(Level.SEVERE, "Unknown Parameter: " + name);
		}
		p_C_RfQ_ID = getRecord_ID();
	}	//	prepare

	/**
	 * 	Process.
	 * 	Create purchase order(s) for the resonse(s) and lines marked as 
	 * 	Selected Winner using the selected Purchase Quantity (in RfQ Line Quantity) . 
	 * 	If a Response is marked as Selected Winner, all lines are created 
	 * 	(and Selected Winner of other responses ignored).  
	 * 	If there is no response marked as Selected Winner, the lines are used.
	 *	@return message
	 */
	protected String doIt () throws Exception
	{
		MRfQ rfq = new MRfQ (getCtx(), p_C_RfQ_ID, get_TrxName());
		int lastAD_Org_ID = 0;
		if (rfq.get_ID() == 0)
			throw new IllegalArgumentException("No RfQ found");
		if (log.isLoggable(Level.INFO)) log.info(rfq.toString());
		
		//	Complete 
		//MRfQResponse[] responses = rfq.getResponses(true, true);
		List<FTUMRfQResponse> responses = new Query(getCtx(), FTUMRfQResponse.Table_Name, "C_RfQ_ID = ? AND IsComplete = 'Y'", get_TrxName())
				.setOnlyActiveRecords(true)
				.setParameters(rfq.get_ID())
				.setOrderBy("C_PaymentTerm_ID, Price")
				.list();
		
		if (log.isLoggable(Level.CONFIG)) log.config("#Responses=" + responses.size());
		if (responses.size() == 0)
			throw new IllegalArgumentException("No completed RfQ Responses found");
		
		//	Winner for entire RfQ
		for (int i = 0; i < responses.size(); i++)
		{
			FTUMRfQResponse response = responses.get(i);
			if (response.getC_Order_ID() > 0)
				continue;
			if (!response.isSelectedWinner())
				continue;
			//
			MOrder order = null;
			MBPartner bp = new MBPartner(getCtx(), response.getC_BPartner_ID(), get_TrxName());
			if (log.isLoggable(Level.CONFIG)) log.config("Winner=" + bp);
			//
			//MRfQResponseLine[] lines = response.getLines(false);
			String where = "C_RfQResponseLine.C_RfQResponse_ID = ?";
			List<FTUMRfQResponseLine> lines = new Query(response.getCtx(), FTUMRfQResponseLine.Table_Name, where, response.get_TrxName())
					.addJoinClause("INNER JOIN C_RfQLine rfql ON rfql.C_RfQLine_ID = C_RfQResponseLine.C_RfQLine_ID")
					.setOnlyActiveRecords(true)
					.setParameters(response.get_ID())
					.setOrderBy("rfql.AD_Org_ID")
					.list();
			for (int j = 0; j < lines.size(); j++)
			{				
				//	Respones Line
				FTUMRfQResponseLine line = lines.get(j);
				if (!line.isActive())
					continue;
				
				MRfQLine rfqline = new MRfQLine(getCtx(), line.getRfQLine().get_ID(), line.get_TrxName());
				int M_RequisitionLine_ID = rfqline.get_ValueAsInt(MRequisitionLine.COLUMNNAME_M_RequisitionLine_ID);
				
				MRequisitionLine reqLine = null;
				MRequisition requisition = null;
				
				if (M_RequisitionLine_ID != 0)
					reqLine = new MRequisitionLine(rfqline.getCtx(), M_RequisitionLine_ID, rfqline.get_TrxName());
				
				if (reqLine != null)
					requisition = reqLine.getParent();
				
				if (lastAD_Org_ID != rfqline.getAD_Org_ID())
				{
					lastAD_Org_ID = rfqline.getAD_Org_ID();
					order = null;
				}
				
				if (order == null)
				{
					int C_PaymentTerm_ID = response.get_ValueAsInt(MOrder.COLUMNNAME_C_PaymentTerm_ID);
					order = new MOrder (getCtx(), 0, get_TrxName());
					order.setClientOrg(response.getAD_Client_ID(), lastAD_Org_ID);
					order.setIsSOTrx(false);
					System.out.println("Tipo de Documento: "+p_C_DocType_ID);
					if (p_C_DocType_ID > 0)
						order.setC_DocTypeTarget_ID(p_C_DocType_ID);
					else
						order.setC_DocTypeTarget_ID();
					
					if (requisition != null)
						order.setM_Warehouse_ID(requisition.getM_Warehouse_ID());
					
					order.setBPartner(bp);
					order.setC_BPartner_Location_ID(response.getC_BPartner_Location_ID());
					order.setSalesRep_ID(rfq.getSalesRep_ID());
					if (rfq.get_ValueAsInt("M_PriceList_ID")>0)
						order.setM_PriceList_ID(rfq.get_ValueAsInt("M_PriceList_ID"));
					if (C_PaymentTerm_ID != 0)
						order.setC_PaymentTerm_ID(C_PaymentTerm_ID);
					
					if (response.getDateWorkComplete() != null) {
						order.setDatePromised(response.getDateWorkComplete());
						if(requisition != null)
						{
							order.setUser1_ID(requisition.get_ValueAsInt("User1_ID"));
							order.setC_Activity_ID(requisition.get_ValueAsInt("C_Activity_ID"));
						}
						order.setDescription(rfq.getDescription());}
					else if (rfq.getDateWorkComplete() != null)
					order.setDatePromised(rfq.getDateWorkComplete());
					if(requisition != null)
					{
						order.setUser1_ID(requisition.get_ValueAsInt("User1_ID"));
						order.setC_Activity_ID(requisition.get_ValueAsInt("C_Activity_ID"));
					}
					order.setDescription(rfq.getDescription());					
					order.saveEx();
				}
				
				MRfQResponseLineQty[] qtys = line.getQtys(false);
				//	Response Line Qty
				for (int k = 0; k < qtys.length; k++)
				{
					MRfQResponseLineQty qty = qtys[k];
					//	Create PO Lline for all Purchase Line Qtys
					if (qty.getRfQLineQty().isActive() && qty.getRfQLineQty().isPurchaseQty())
					{
						MOrderLine ol = new MOrderLine (order);
						ol.setM_Product_ID(line.getRfQLine().getM_Product_ID(), 
							qty.getRfQLineQty().getC_UOM_ID());
						if (requisition != null)
							ol.setM_Warehouse_ID(requisition.getM_Warehouse_ID());
						ol.setDescription(line.getDescription());
						ol.setQty(qty.getRfQLineQty().getQty());
						BigDecimal price = qty.getNetAmt();
						if(price == null || price.compareTo(BigDecimal.ZERO) == 0)
							price = BigDecimal.ZERO;
						ol.setPrice();
						ol.setPrice(price);
						
						if(reqLine != null) {
							
							//MRequisitionLine mrl = new MRequisitionLine(getCtx(), rfqline.get_ValueAsInt("M_RequisitionLine_ID"), null);
							
							if(reqLine.get_ValueAsInt("C_Activity_ID") != 0) {
								ol.setC_Activity_ID(reqLine.get_ValueAsInt("C_Activity_ID"));
							}
							
							if(reqLine.get_ValueAsInt("User1_ID") != 0) {
								ol.setUser1_ID(reqLine.get_ValueAsInt("User1_ID"));
							}
						}
						
						ol.saveEx();
						
						if (reqLine != null)
						{
							reqLine.setC_OrderLine_ID(ol.get_ID());
							reqLine.saveEx();
						}
					}
				}
			}
			response.setC_Order_ID(order.getC_Order_ID());
			response.saveEx();
			return order.getDocumentNo();
		}

		
		//	Selected Winner on Line Level
		int noOrders = 0;
		int lastC_PaymentTerm_ID = 0;
		lastAD_Org_ID = 0;
		for (int i = 0; i < responses.size(); i++)
		{
			FTUMRfQResponse response = responses.get(i);
			if (response.getC_Order_ID() > 0)
				continue;
			MBPartner bp = null;
			MOrder order = null;
			//	For all Response Lines
			String where = "C_RfQResponse_ID = ?";
			List<FTUMRfQResponseLine> lines = new Query(response.getCtx(), FTUMRfQResponseLine.Table_Name, where, response.get_TrxName())
					.setOnlyActiveRecords(true)
					.addJoinClause("INNER JOIN C_RfQLine rfql ON rfql.C_RfQLine_ID = C_RfQResponseLine.C_RfQLine_ID")
					.setOrderBy("rfql.AD_Org_ID")
					.setParameters(response.get_ID())
					.list();
			for (int j = 0; j < lines.size(); j++)
			{
				FTUMRfQResponseLine line = lines.get(j);
				if (!line.isActive() || !line.isSelectedWinner())
					continue;
				
				MRfQLine rfqLine = (MRfQLine) line.getC_RfQLine();
				int M_RequisitionLine_ID = rfqLine.get_ValueAsInt(MRequisitionLine.COLUMNNAME_M_RequisitionLine_ID);
				MRequisitionLine rqLine = null;
				MRequisition requisition = null;
				
				if (M_RequisitionLine_ID != 0)
					rqLine = new MRequisitionLine(rfqLine.getCtx(), M_RequisitionLine_ID, rfqLine.get_TrxName());
				if (rqLine != null)
					requisition = (MRequisition) rqLine.getM_Requisition();
				
				//	New/different BP
				if (bp == null || bp.getC_BPartner_ID() != response.getC_BPartner_ID()
						|| lastAD_Org_ID != rfqLine.getAD_Org_ID()
						|| lastC_PaymentTerm_ID != response.get_ValueAsInt(MOrder.COLUMNNAME_C_PaymentTerm_ID))
				{
					bp = new MBPartner(getCtx(), response.getC_BPartner_ID(), get_TrxName());
					lastAD_Org_ID = rfqLine.getAD_Org_ID();
					lastC_PaymentTerm_ID = response.get_ValueAsInt(MOrder.COLUMNNAME_C_PaymentTerm_ID);
					order = null;
				}
				if (log.isLoggable(Level.CONFIG)) log.config("Line=" + line + ", Winner=" + bp);
				//	New Order
				if (order == null)
				{
					order = new MOrder (getCtx(), 0, get_TrxName());
					order.setClientOrg(response.getAD_Client_ID(), lastAD_Org_ID);
					order.setIsSOTrx(false);
					order.setC_DocTypeTarget_ID(p_C_DocType_ID);
					order.setBPartner(bp);
					order.setC_BPartner_Location_ID(response.getC_BPartner_Location_ID());
					order.setSalesRep_ID(rfq.getSalesRep_ID());
					order.setDescription(rfq.getDescription());
					order.setUser1_ID(requisition.get_ValueAsInt("User1_ID"));
					order.setC_Activity_ID(requisition.get_ValueAsInt("C_Activity_ID"));
					order.setDescription(rfq.getDescription());
					if (requisition != null)
						order.setM_Warehouse_ID(requisition.getM_Warehouse_ID());
					if (lastC_PaymentTerm_ID != 0)
						order.setC_PaymentTerm_ID(lastC_PaymentTerm_ID);

					if (rfq.get_ValueAsInt("M_PriceList_ID")>0)
						order.setM_PriceList_ID(rfq.get_ValueAsInt("M_PriceList_ID"));
					
					order.saveEx();
					noOrders++;
					addBufferLog(0, null, null, order.getDocumentNo(), order.get_Table_ID(), order.getC_Order_ID());
				}
				//	For all Qtys
				MRfQResponseLineQty[] qtys = line.getQtys(false);
				for (int k = 0; k < qtys.length; k++)
				{
					MRfQResponseLineQty qty = qtys[k];
					if (qty.getRfQLineQty().isActive() && qty.getRfQLineQty().isPurchaseQty())
					{
						MOrderLine ol = new MOrderLine (order);
						BigDecimal qtyOrdered = (BigDecimal) qty.get_Value("QuotationQuantity");
						if(qtyOrdered == null)
							qtyOrdered = qty.getRfQLineQty().getQty();
						ol.setM_Product_ID(line.getRfQLine().getM_Product_ID(), 
							qty.getRfQLineQty().getC_UOM_ID());
						
						if (requisition != null)
							ol.setM_Warehouse_ID(requisition.getM_Warehouse_ID());
						
						ol.setDescription(line.getDescription());
						ol.setQty(qtyOrdered);
						BigDecimal price = qty.getNetAmt();
						ol.setPrice();
						ol.setPrice(price);
						if(rqLine != null) {
							
							//MRequisitionLine mrl = new MRequisitionLine(getCtx(), rfqline.get_ValueAsInt("M_RequisitionLine_ID"), null);
							
							if(rqLine.get_ValueAsInt("C_Activity_ID") != 0) {
								ol.setC_Activity_ID(rqLine.get_ValueAsInt("C_Activity_ID"));
							}
							
							if(rqLine.get_ValueAsInt("User1_ID") != 0) {
								ol.setUser1_ID(rqLine.get_ValueAsInt("User1_ID"));
							}
						}
						ol.saveEx();
						
						if (rqLine != null)
						{
							rqLine.setC_OrderLine_ID(ol.get_ID());
							rqLine.saveEx();
						}
					}
				}	//	for all Qtys
			}	//	for all Response Lines
			if (order != null)
			{
				response.setC_Order_ID(order.getC_Order_ID());
				response.saveEx();
			}
		}
		
		if (p_CloseIt)
			closeRfq();
		
		StringBuilder msgreturn = new StringBuilder("#").append(noOrders);
		return msgreturn.toString();
	}	//	doIt
	
	protected String closeRfq() {
		MRfQ rfq = new MRfQ (getCtx(), p_C_RfQ_ID, get_TrxName());
		if (rfq.get_ID() == 0)
			throw new IllegalArgumentException("No RfQ found");
		if (log.isLoggable(Level.INFO)) log.info("doIt - " + rfq);
		//
		rfq.setProcessed(true);
		rfq.saveEx();
		//
		int counter = 0;
		MRfQResponse[] responses = rfq.getResponses (false, false);
		for (int i = 0; i < responses.length; i++)
		{
			responses[i].setProcessed(true);
			responses[i].saveEx();
			counter++;
		}
		//
		StringBuilder msgreturn = new StringBuilder("# ").append(counter);
		return msgreturn.toString();
	}
}