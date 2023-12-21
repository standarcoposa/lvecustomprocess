/**
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Copyright (C) 2020 Frontuari, C.A. <http://frontuari.net> and contributors (see README.md file).
 */

package net.frontuari.lvecustomprocess.component;

import net.frontuari.lvecustomprocess.base.FTUProcessFactory;
import net.frontuari.lvecustomprocess.process.AllocationReset;
import net.frontuari.lvecustomprocess.process.FTUAging;
import net.frontuari.lvecustomprocess.process.FTUCommissionAPInvoice;
import net.frontuari.lvecustomprocess.process.FTUCommissionCalc;
import net.frontuari.lvecustomprocess.process.FTUFinStatement;
import net.frontuari.lvecustomprocess.process.FTUInvoiceWriteOff;
import net.frontuari.lvecustomprocess.process.FTUReplenishReport;
import net.frontuari.lvecustomprocess.process.FTUReplenishReportProduction;
import net.frontuari.lvecustomprocess.process.FTURfQCreate;
import net.frontuari.lvecustomprocess.process.FTURfQCreatePO;
import net.frontuari.lvecustomprocess.process.FTURfQResponseRank;
import net.frontuari.lvecustomprocess.process.InvoiceNGL;
import net.frontuari.lvecustomprocess.process.MatchInvDelete;
import net.frontuari.lvecustomprocess.process.PaymentWriteOff;
import net.frontuari.lvecustomprocess.process.RecreateStorageReservation;
import net.frontuari.lvecustomprocess.process.RequisitionPOCreate;

/**
 * Process Factory
 */
public class ProcessFactory extends FTUProcessFactory {

	/**
	 * For initialize class. Register the process to build
	 * 
	 * <pre>
	 * protected void initialize() {
	 * 	registerProcess(PPrintPluginInfo.class);
	 * }
	 * </pre>
	 */
	@Override
	protected void initialize() {
		registerProcess(FTURfQCreate.class);
		registerProcess(FTURfQCreatePO.class);
		registerProcess(RequisitionPOCreate.class);
		registerProcess(FTURfQResponseRank.class);
		registerProcess(FTUAging.class);
		registerProcess(PaymentWriteOff.class);
		registerProcess(FTUInvoiceWriteOff.class);
		registerProcess(FTUReplenishReport.class);
		registerProcess(FTUReplenishReportProduction.class);
		registerProcess(RecreateStorageReservation.class);
		registerProcess(MatchInvDelete.class);
		registerProcess(FTUFinStatement.class);
		registerProcess(FTUCommissionCalc.class);
		registerProcess(FTUCommissionAPInvoice.class);
		registerProcess(AllocationReset.class);
		registerProcess(InvoiceNGL.class);
	}

}
