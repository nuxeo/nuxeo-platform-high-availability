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
 * Contributors: Sun Seng David TAN (a.k.a. sunix) <stan@nuxeo.com>
 */
package org.nuxeo.ecm.platform.lock.api;

import java.net.URI;

/**
 * Exception thrown when the system is trying to get the lock but couldn't find
 * it.
 *
 * @author Sun Seng David TAN (a.k.a. sunix) <stan@nuxeo.com>
 */
public class NoSuchLockException extends LockException {
    private static final long serialVersionUID = 1L;

    public NoSuchLockException(Throwable e, URI r) {
        super("No Such lock", e, r);
    }

}
