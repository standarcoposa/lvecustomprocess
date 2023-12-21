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

import net.frontuari.lvecustomprocess.base.FTUModelFactory;
import net.frontuari.lvecustomprocess.model.FTUMRfQResponse;
import net.frontuari.lvecustomprocess.model.FTUMRfQResponseLine;
import net.frontuari.lvecustomprocess.model.MFTUAging;

/**
 * Model Factory
 */
public class ModelFactory extends FTUModelFactory {

	/**
	 * For initialize class. Register the models to build
	 * 
	 * <pre>
	 * protected void initialize() {
	 * 	registerModel(MTableExample.Table_Name, MTableExample.class);
	 * }
	 * </pre>
	 */
	@Override
	protected void initialize() {
		registerModel(FTUMRfQResponse.Table_Name, FTUMRfQResponse.class);
		registerModel(FTUMRfQResponseLine.Table_Name, FTUMRfQResponseLine.class);
		registerModel(MFTUAging.Table_Name, MFTUAging.class);
	}

}
