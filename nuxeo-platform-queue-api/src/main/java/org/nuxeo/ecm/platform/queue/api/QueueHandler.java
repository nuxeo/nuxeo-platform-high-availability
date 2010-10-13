/*
 * (C) Copyright 2010 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials are made
 * available under the terms of the GNU Lesser General Public License (LGPL)
 * version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * Contributors:    Stephane Lacoin at Nuxeo (aka matic),
 *                  Sun Seng David TAN (a.k.a. sunix) <stan@nuxeo.com>
 */
package org.nuxeo.ecm.platform.queue.api;

import org.nuxeo.ecm.core.management.guards.Guarded;
import org.nuxeo.runtime.transaction.Transacted;

import java.io.Serializable;
import java.net.URI;

/**
 * Handle content into dedicated queues and handle
 *
 * @author Sun Seng David TAN (a.k.a. sunix) <stan@nuxeo.com>
 * @author "Stephane Lacoin at Nuxeo (aka matic)"
 */

public interface QueueHandler {

    /**
     * Inject and process new content in queue
     *
     * @param content the content
     */
    @Transacted
    @Guarded
    <C extends Serializable> void newContent(URI owner, URI contentName,  C content);

    /**
     * Register and process content if unknown.
     *
     * @param ownerName the context owner
     * @param resource the content name
     */
    @Transacted
    @Guarded
    <C extends Serializable> void newContentIfUnknown(URI ownerName, URI contentName,  C content);

    /**
     * Generate a name referencing an unique content
     *
     * @param queueName
     * @param contentName
     * @return
     */
    @Guarded
    URI newName(String queueName, String contentName);

    /**
     * Notify content process termination, blacklist content for a while
     *
     * @param queueName
     * @param contentName
     * @return
     */
    @Transacted
    <C extends Serializable> QueueInfo<C> blacklist(URI contentName);

    /**
     * Retry content processing
     *
     * @param contentName
     * @return
     */
    @Transacted
    @Guarded
    <C extends Serializable> QueueInfo<C> retry(URI contentName);

    /**
     * Remove content from the queue
     *
     * @param <C>
     * @param contentName
     * @return
     */
    @Transacted
    <C extends Serializable> QueueInfo<C> purge(URI contentName);

    /**
     * Indicate that content processing has failed
     */
    @Transacted
    <C extends Serializable> QueueInfo<C> error(URI contentName, Throwable error);
}
