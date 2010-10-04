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
 *     "Stephane Lacoin [aka matic] <slacoin at nuxeo.com>"
 */
package org.nuxeo.ecm.platform.queue.core.storage;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.management.storage.DocumentStoreSessionRunner;



/**
 * @author "Stephane Lacoin [aka matic] <slacoin at nuxeo.com>"
 *
 */
public class StorageHandler implements InvocationHandler{

    protected StorageManager mgr;

    protected DocumentStoreSessionRunner runner;

   public static <T> T newProxy(StorageManager mgr, Class<T> itf) {
         InvocationHandler h = new StorageHandler(mgr);
         return itf.cast(Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[] { itf }, h));
    }

   protected StorageHandler(StorageManager mgr) {
           this.mgr = mgr;
   }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        ReifiedMethodRunner runner = new ReifiedMethodRunner(method, args);
        runner.runUnrestricted();
        if (runner.rvalue instanceof Throwable) {
            throw (Throwable) runner.rvalue;
        }
        return runner.rvalue;
    }

    protected  class ReifiedMethodRunner extends DocumentStoreSessionRunner {
        protected final Method m;
        protected final Object[] args;
        ReifiedMethodRunner(Method m, Object[] args) {
            this.m = m;
            this.args = args;
        }
        protected Object rvalue;
        @Override
        public void run() throws ClientException {
            mgr.setSession(session);
            try {
                rvalue = m.invoke(mgr, args);
            } catch (InvocationTargetException error) {
                rvalue = error.getCause();
            } catch (Throwable e) {
                rvalue = e;
            }
        }
    }




}
