/*
 * (C) Copyright 2010 Nuxeo SAS (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Nuxeo - initial API and implementation
 */

package org.nuxeo.ecm.platform.queue.core;


public class DocumentQueueConstants {

    public static final String QUEUE_ROOT_NAME = "queues";

    public static final String QUEUE_ROOT_TYPE = "QueueRoot";

    public static final String QUEUE_TYPE = "Queue";

    public static final String QUEUE_ITEM_TYPE = "QueueItem";

    public static final String QUEUEITEM_OWNER = "owner";

    public static final String QUEUEITEM_SERVERID = "serverId";

    public static final String QUEUEITEM_SCHEMA = "queueitem";

    public static final String QUEUEITEM_EXECUTE_TIME = "executeTime";

    public static final String QUEUEITEM_BLACKLIST_TIME = "blacklistTime";

    public static final String QUEUEITEM_CONTENT = "content";

    public static final String QUEUEITEM_EXECUTION_COUNT_PROPERTY = "queueitem:executionCount";

    private DocumentQueueConstants() {
    }

}
