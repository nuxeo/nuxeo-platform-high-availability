/*
 * (C) Copyright 2006-2008 Nuxeo SAS (http://nuxeo.com/) and contributors.
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
 *     matic
 */

package org.nuxeo.ecm.platform.queue.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.nuxeo.ecm.platform.queue.api.Transacted;
import org.nuxeo.runtime.transaction.TransactionHelper;

/**
 * Start a user transaction if needed
 *
 * @author matic
 *
 * @param <T>
 */
public class TransactedInstanceHandler<T> implements InvocationHandler{

    protected T object;

    public static <T> T newProxy(T object, Class<T> itf) {
         InvocationHandler h = new TransactedInstanceHandler<T>(object);
         return itf.cast(Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[] { itf }, h));
    }

    protected TransactedInstanceHandler(T object) {
        this.object = object;
    }

    protected boolean requireTransaction(Method m) {
        if (!m.isAnnotationPresent(Transacted.class) && !m.getClass().isAnnotationPresent(Transacted.class)) {
            return false;
        }
        return !TransactionHelper.isTransactionActive();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (!requireTransaction(method)) {
            return method.invoke(object, args);
        }
        TransactionHelper.startTransaction();
        try {
            return method.invoke(object, args);
        } catch (Throwable e) {
            TransactionHelper.setTransactionRollbackOnly();
            throw e;
        } finally {
            TransactionHelper.commitOrRollbackTransaction();
        }
    }

}
