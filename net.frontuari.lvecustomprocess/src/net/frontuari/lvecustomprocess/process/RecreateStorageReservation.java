/******************************************************************************
 * Project: Trek Global ERP                                                   *
 * Copyright (C) 2009-2018 Trek Global Corporation                			  *
 * This program is free software; you can redistribute it and/or modify it    *
 * under the terms version 2 of the GNU General Public License as published   *
 * by the Free Software Foundation. This program is distributed in the hope   *
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied *
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.           *
 * See the GNU General Public License for more details.                       *
 * You should have received a copy of the GNU General Public License along    *
 * with this program; if not, write to the Free Software Foundation, Inc.,    *
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.                     *
 *****************************************************************************/
package net.frontuari.lvecustomprocess.process;

import java.util.logging.Level;

import org.compiere.process.ProcessInfoParameter;
import org.compiere.util.DB;
import org.compiere.util.Env;

import net.frontuari.lvecustomprocess.base.FTUProcess;

/***
 * With this process can recreate storage reservation filter by Org, Warehouse, Product Category or one Product
 * @author Jorge Colmenarez, 2022-01-17 17:47
 */
public class RecreateStorageReservation extends FTUProcess {

	/**	Client 						*/
	private int m_AD_Client_ID 			= 0;
	/**	Organization				*/
	private int m_AD_Org_ID				= 0;
	/**	Warehouse					*/
	private int m_M_Warehouse_ID		= 0;
	/**	Product Category			*/
	private int m_M_Product_Category_ID	= 0;
	/**	Product	*/
	private int m_M_Product_ID			= 0;
	
	public RecreateStorageReservation() {
	}

	@Override
	protected void prepare() {
		ProcessInfoParameter[] para = getParameter();
		for (int i = 0; i < para.length; i++)
		{
			String name = para[i].getParameterName();
			if (para[i].getParameter() == null && para[i].getParameter_To() == null)
				;
			else if (name.equals("AD_Client_ID"))
				m_AD_Client_ID = para[i].getParameterAsInt();
			else if (name.equals("AD_Org_ID"))
				m_AD_Org_ID = para[i].getParameterAsInt();
			else if (name.equals("M_Warehouse_ID"))
				m_M_Warehouse_ID = para[i].getParameterAsInt();
			else if (name.equals("M_Product_Category_ID"))
				m_M_Product_Category_ID = para[i].getParameterAsInt();
			else if (name.equals("M_Product_ID"))
				m_M_Product_ID = para[i].getParameterAsInt();
			else
				log.log(Level.SEVERE, "Unknown Parameter: " + name);
		}
	}

	@Override
	protected String doIt() throws Exception {
		//	Cleaner
		cleanOrderReservation();
		
		String sql = ""
				+ "SELECT COUNT(*) "
				+ "FROM "
				//	Sales/Purchase Order
				+ "(";
				//	Inyect SQL
				sql += getSQLOrder();
				//	Continue SQL
				sql += ") y "
				+ "FULL OUTER JOIN "
				+ "( "
				+ "SELECT s.Qty AS StorageQtyreserved, "
				+ "       s.M_Warehouse_ID, "
				+ "       s.M_Product_ID, "
				+ "       s.M_AttributeSetInstance_ID, "
				+ "       s.IsSOTrx, "
				+ "       s.AD_Client_ID "
				+ "FROM   M_StorageReservation s "
				+ "JOIN M_Product p ON (s.M_Product_ID = p.M_Product_ID) "
				+ "WHERE  s.AD_Client_ID = ? "
				+ "       AND s.Qty != 0 ";
				//	Add filters
				if(m_AD_Org_ID > 0)
					sql += " AND (s.AD_Org_ID = "+m_AD_Org_ID+") ";
				if(m_M_Warehouse_ID > 0)
					sql += " AND (s.M_Warehouse_ID = "+m_M_Warehouse_ID+") ";
				if(m_M_Product_Category_ID > 0)
					sql += " AND (p.M_Product_Category_ID = "+m_M_Product_Category_ID+") ";
				if(m_M_Product_ID > 0)
					sql += " AND (s.M_Product_ID = "+m_M_Product_ID+") ";
				//	End filters
		sql		+= ") x "
				+ "    ON y.M_Warehouse_ID = x.M_Warehouse_ID "
				+ "       AND x.M_Product_ID = y.M_Product_ID "
				+ "       AND x.M_AttributeSetInstance_ID = y.M_AttributeSetInstance_ID "
				+ "       AND x.IsSOTrx = y.IsSOTrx "
				+ "WHERE  COALESCE(x.StorageQtyReserved, 0) <> COALESCE(y.OrderQtyReserved, 0)";

		int wrongReservations = DB.getSQLValueEx(get_TrxName(), sql, m_AD_Client_ID, m_AD_Client_ID, m_AD_Client_ID, m_AD_Client_ID);
		
		int noInserted = 0;
		if (wrongReservations > 0) {
			
			log.warning(wrongReservations + " wrong reservation records found");
			
			String deleteSql = "DELETE FROM M_StorageReservation s "
					+ " WHERE s.AD_Client_ID=? ";
					//	Add filters
					if(m_AD_Org_ID > 0)
						deleteSql += " AND (s.AD_Org_ID = "+m_AD_Org_ID+") ";
					if(m_M_Warehouse_ID > 0)
						deleteSql += " AND (s.M_Warehouse_ID = "+m_M_Warehouse_ID+") ";
					if(m_M_Product_Category_ID > 0)
						deleteSql += " AND (s.M_Product_ID IN (SELECT p.M_Product_ID FROM M_Product p WHERE p.M_Product_Category_ID = "+m_M_Product_Category_ID+")) ";
					if(m_M_Product_ID > 0)
						deleteSql += " AND (s.M_Product_ID = "+m_M_Product_ID+") ";
					//	End filters
					
			int no = DB.executeUpdateEx(deleteSql, new Object[]{m_AD_Client_ID}, get_TrxName());
			log.warning(no + " reservation records deleted");
			
			String ins = ""
					+ "INSERT INTO M_StorageReservation "
					+ "            (Qty, "
					+ "             M_Warehouse_ID, "
					+ "             M_Product_ID, "
					+ "             M_AttributeSetInstance_ID, "
					+ "             IsSOTrx, "
					+ "             AD_Client_ID, "
					+ "             AD_Org_ID, "
					+ "             Created, "
					+ "             CreatedBy, "
					+ "             Updated, "
					+ "             UpdatedBy, "
					+ "             IsActive, "
					+ "             M_StorageReservation_UU) ";
					//	Inyect SQL
					ins += "SELECT *,SysDate,?,SysDate,?,'Y',generate_uuid() FROM (";
					ins += getSQLOrder();
					ins += ") x ";
					
			noInserted = DB.executeUpdateEx(ins, new Object[]{Env.getAD_User_ID(getCtx()), Env.getAD_User_ID(getCtx()), m_AD_Client_ID, m_AD_Client_ID, m_AD_Client_ID}, get_TrxName());
			log.warning(noInserted + " reservation records inserted");
		}

		return noInserted + " @Inserted@";
	}
	
	/***
	 * Clean Order Reservation from Document Voided or Reversed
	 * @author Jorge Colmenarez, 2022-01-20 11:05
	 */
	public void cleanOrderReservation()
	{
		//	SQL Purchase/Sales Order
		String sqlorder = "UPDATE C_OrderLine SET QtyReserved = 0 WHERE C_OrderLine_ID IN (";
		sqlorder += "SELECT ol.C_OrderLine_ID FROM C_OrderLine ol "
				+ "JOIN C_Order o ON (ol.C_Order_ID = o.C_Order_ID) "
				+ "JOIN M_Product p ON (ol.M_Product_ID = p.M_Product_ID) "
				+ "JOIN M_Warehouse w ON (w.M_Warehouse_ID = o.M_Warehouse_ID) "
				+ "WHERE ol.QtyReserved != 0 AND o.DocStatus IN ('VO','RE') AND o.AD_Client_ID = ? ";
				//	Add filters
				if(m_AD_Org_ID > 0)
					sqlorder += " AND (w.AD_Org_ID = "+m_AD_Org_ID+") ";
				if(m_M_Warehouse_ID > 0)
					sqlorder += " AND (o.M_Warehouse_ID = "+m_M_Warehouse_ID+") ";
				if(m_M_Product_Category_ID > 0)
					sqlorder += " AND (p.M_Product_Category_ID = "+m_M_Product_Category_ID+") ";
				if(m_M_Product_ID > 0)
					sqlorder += " AND (ol.M_Product_ID = "+m_M_Product_ID+") ";
				sqlorder += ")";
				//	End filters
				
		//	SQL PP Order Header
		String sqlpporder = "UPDATE PP_Order SET QtyReserved = 0 WHERE PP_Order_ID = (";
		sqlpporder += "SELECT o.PP_Order_ID FROM PP_Order o "
				+ "JOIN M_Product p ON (o.M_Product_ID = p.M_Product_ID) "
				+ "JOIN M_Warehouse w ON (w.M_Warehouse_ID = o.M_Warehouse_ID) "
				+ "WHERE o.QtyReserved != 0 AND o.DocStatus IN ('VO','RE') AND o.AD_Client_ID = ? ";
				//	Add filters
				if(m_AD_Org_ID > 0)
					sqlpporder += " AND (w.AD_Org_ID = "+m_AD_Org_ID+") ";
				if(m_M_Warehouse_ID > 0)
					sqlpporder += " AND (o.M_Warehouse_ID = "+m_M_Warehouse_ID+") ";
				if(m_M_Product_Category_ID > 0)
					sqlpporder += " AND (p.M_Product_Category_ID = "+m_M_Product_Category_ID+") ";
				if(m_M_Product_ID > 0)
					sqlpporder += " AND (o.M_Product_ID = "+m_M_Product_ID+") ";
				sqlpporder += ")";
				//	End filters
		
		//	SQL PP Order Line
		String sqlpporderline = "UPDATE PP_Order_BOMLine SET QtyReserved = 0 WHERE PP_Order_BOMLine_ID IN (";
		sqlpporderline += "SELECT ol.PP_Order_BOMLine_ID FROM PP_Order_BOMLine ol "
				+ "JOIN PP_Order o ON (ol.PP_Order_ID = o.PP_Order_ID) "
				+ "JOIN M_Product p ON (ol.M_Product_ID = p.M_Product_ID) "
				+ "JOIN M_Warehouse w ON (w.M_Warehouse_ID = COALESCE(ol.M_WarehouseSource_ID,ol.M_Warehouse_ID)) "
				+ "WHERE ol.QtyReserved != 0 AND o.DocStatus IN ('VO','RE') AND o.AD_Client_ID = ? ";
				//	Add filters
				if(m_AD_Org_ID > 0)
					sqlpporderline += " AND (w.AD_Org_ID = "+m_AD_Org_ID+") ";
				if(m_M_Warehouse_ID > 0)
					sqlpporderline += " AND (o.M_Warehouse_ID = "+m_M_Warehouse_ID+") ";
				if(m_M_Product_Category_ID > 0)
					sqlpporderline += " AND (p.M_Product_Category_ID = "+m_M_Product_Category_ID+") ";
				if(m_M_Product_ID > 0)
					sqlpporderline += " AND (ol.M_Product_ID = "+m_M_Product_ID+") ";
				sqlpporderline += ")";
				//	End filters
				
		// Clean PO/SO
		int cntOrder = DB.executeUpdate(sqlorder, m_AD_Client_ID, get_TrxName());
		int cntPPOrder = DB.executeUpdate(sqlpporder, m_AD_Client_ID, get_TrxName());
		int cntPPOrderLine = DB.executeUpdate(sqlpporderline, m_AD_Client_ID, get_TrxName());
		log.warning(cntOrder + " voided Sales/Purchases orders cleared");
		log.warning(cntPPOrder + " voided PP orders cleared");
		log.warning(cntPPOrderLine + " voided PP order lines cleared");
	}
	
	/***
	 * Get Order with reservation
	 * @author Jorge Colmenarez, 2022-01-20 11:04
	 * @return sql
	 */
	public String getSQLOrder()
	{
		String sql = "SELECT * FROM "
				+ "(SELECT SUM(COALESCE(a.OrderQtyReserved,0)+COALESCE(b.PPOrderQtyReserved,0)+COALESCE(c.PPOrderLineQtyReserved,0)) as OrderQtyReserved,"
				+ "	COALESCE(a.M_Warehouse_ID,b.M_Warehouse_ID,c.M_Warehouse_ID) as M_Warehouse_ID,"
				+ "	COALESCE(a.M_Product_ID,b.M_Product_ID,c.M_Product_ID) as M_Product_ID,"
				+ "	COALESCE(a.M_AttributeSetInstance_ID,b.M_AttributeSetInstance_ID,c.M_AttributeSetInstance_ID) as M_AttributeSetInstance_ID,"
				+ "	COALESCE(a.IsSOTrx,b.IsSOTrx,c.IsSOTrx) as IsSOTrx,"
				+ "	COALESCE(a.AD_Client_ID,b.AD_Client_ID,c.AD_Client_ID) as AD_Client_ID,"
				+ "	COALESCE(a.AD_Org_ID,b.AD_Org_ID,c.AD_Org_ID) as AD_Org_ID "
				+ "FROM ("
				+ "SELECT SUM(ol.QtyReserved) AS OrderQtyReserved, "
				+ "       ol.M_Warehouse_ID, "
				+ "       ol.M_Product_ID, "
				+ "       COALESCE(ol.M_AttributeSetInstance_ID, 0) AS M_AttributeSetInstance_ID, "
				+ "       o.IsSOTrx, "
				+ "       w.AD_Client_ID, "
				+ "       w.AD_Org_ID "
				+ "FROM   C_OrderLine ol "
				+ " 	  JOIN M_Product p ON (ol.M_Product_ID = p.M_Product_ID) "
				+ "       JOIN C_Order o ON ( ol.C_Order_ID = o.C_Order_ID ) "
				+ "       JOIN M_Warehouse w ON ( w.M_Warehouse_ID = o.M_Warehouse_ID ) "
				+ "WHERE  ol.ad_client_ID = ? "
				+ "       AND ol.qtyreserved != 0 "
				+ "       AND o.docstatus NOT IN ( 'VO', 'RE' ) ";
				//	Add filters
				if(m_AD_Org_ID > 0)
					sql += " AND (w.AD_Org_ID = "+m_AD_Org_ID+") ";
				if(m_M_Warehouse_ID > 0)
					sql += " AND (o.M_Warehouse_ID = "+m_M_Warehouse_ID+") ";
				if(m_M_Product_Category_ID > 0)
					sql += " AND (p.M_Product_Category_ID = "+m_M_Product_Category_ID+") ";
				if(m_M_Product_ID > 0)
					sql += " AND (ol.M_Product_ID = "+m_M_Product_ID+") ";
				//	End filters
		sql += " GROUP BY 2,3,4,5,6,7) a "
				//	PP_Order Header
				+ "FULL OUTER JOIN (SELECT "
				+ "	SUM(o.QtyReserved) AS PPOrderQtyReserved,"
				+ "	o.M_Warehouse_ID,"
				+ "	o.M_Product_ID, "
				+ "	COALESCE(o.M_AttributeSetInstance_ID, 0) AS M_AttributeSetInstance_ID, "
				+ "	'N' AS IsSOTrx,"
				+ "	w.AD_Client_ID, "
				+ " w.AD_Org_ID "
				+ "FROM PP_Order o "
				+ "JOIN M_Product p ON (o.M_Product_ID = p.M_Product_ID) "
				+ "JOIN M_Warehouse w ON (o.M_Warehouse_ID = w.M_Warehouse_ID) "
				+ "WHERE o.AD_Client_ID = ? "
				+ "AND o.QtyReserved != 0 "
				+ "AND o.DocStatus NOT IN ('VO','RE') ";
				//		Add filters
				if(m_AD_Org_ID > 0)
					sql += " AND (w.AD_Org_ID = "+m_AD_Org_ID+") ";
				if(m_M_Warehouse_ID > 0)
					sql += " AND (o.M_Warehouse_ID = "+m_M_Warehouse_ID+") ";
				if(m_M_Product_Category_ID > 0)
					sql += " AND (p.M_Product_Category_ID = "+m_M_Product_Category_ID+") ";
				if(m_M_Product_ID > 0)
					sql += " AND (o.M_Product_ID = "+m_M_Product_ID+") ";
				//	End filters
				sql += "GROUP BY 2,3,4,5,6,7) b on a.M_Warehouse_ID = b.M_Warehouse_ID "
				+ "       AND a.M_Product_ID = b.M_Product_ID "
				+ "       AND a.M_AttributeSetInstance_ID = b.M_AttributeSetInstance_ID "
				+ "       AND a.IsSOTrx = b.IsSOTrx "
				//	PP_Order Line
				+ "FULL OUTER JOIN (SELECT "
				+ "	SUM(obl.QtyReserved) AS PPOrderLineQtyReserved,"
				+ "	COALESCE(obl.M_WarehouseSource_ID,obl.M_Warehouse_ID) AS M_Warehouse_ID,"
				+ "	obl.M_Product_ID, "
				+ "	COALESCE(obl.M_AttributeSetInstance_ID, 0) AS M_AttributeSetInstance_ID, "
				+ "	'Y' AS IsSOTrx,"
				+ "	w.AD_Client_ID, "
				+ "    w.AD_Org_ID "
				+ "FROM PP_Order o "
				+ "JOIN PP_Order_BOMLine obl ON (o.PP_Order_ID = obl.PP_Order_ID) "
				+ "JOIN M_Product p ON (obl.M_Product_ID = p.M_Product_ID) "
				+ "JOIN M_Warehouse w ON (COALESCE(obl.M_WarehouseSource_ID,obl.M_Warehouse_ID) = w.M_Warehouse_ID) "
				+ "WHERE o.AD_Client_ID = ? "
				+ "AND obl.QtyReserved != 0 "
				+ "AND o.DocStatus NOT IN ('VO','RE') ";
				//		Add filters
				if(m_AD_Org_ID > 0)
					sql += " AND (w.AD_Org_ID = "+m_AD_Org_ID+") ";
				if(m_M_Warehouse_ID > 0)
					sql += " AND (COALESCE(obl.M_WarehouseSource_ID,obl.M_Warehouse_ID) = "+m_M_Warehouse_ID+") ";
				if(m_M_Product_Category_ID > 0)
					sql += " AND (p.M_Product_Category_ID = "+m_M_Product_Category_ID+") ";
				if(m_M_Product_ID > 0)
					sql += " AND (obl.M_Product_ID = "+m_M_Product_ID+") ";
				//	End filters
				sql += "GROUP BY 2,3,4,5,6,7) c on a.M_Warehouse_ID = c.M_Warehouse_ID "
				+ "       AND a.M_Product_ID = c.M_Product_ID "
				+ "       AND a.M_AttributeSetInstance_ID = c.M_AttributeSetInstance_ID "
				+ "       AND a.IsSOTrx = c.IsSOTrx "
				+ "GROUP BY 2,3,4,5,6,7) AS y";
		return sql;
	}

}
