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

package net.frontuari.lvecustomprocess.base;

import org.compiere.model.PO;
import org.osgi.service.event.Event;

/**
 * Class for custom event
 */
public abstract class FTUEvent {

	private PO po;
	private Event event;

	/**
	 * Gets the persistent object
	 * 
	 * @return Persistent objectPersistent object
	 */
	protected PO getPO() {
		return po;
	}

	/**
	 * Gets the event
	 * 
	 * @return Event
	 */
	protected Event getEvent() {
		return event;
	}

	/**
	 * Gets event type
	 * 
	 * @return Event type. Example IEventTopics.DOC_AFTER_COMPLETE
	 */
	protected String getEventType() {
		if (event == null)
			return null;
		return event.getTopic();
	}

	/**
	 * Executes the event in CustomEventFactory
	 * 
	 * @param po    Persistent object
	 * @param event Event
	 */
	protected void doHandleEvent(PO po, Event event) {
		this.po = po;
		this.event = event;
		doHandleEvent();
	}

	/**
	 * Custom event execution
	 */
	protected abstract void doHandleEvent();

}
