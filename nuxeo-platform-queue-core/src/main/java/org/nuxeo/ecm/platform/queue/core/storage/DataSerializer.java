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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.impl.blob.InputStreamBlob;
import org.nuxeo.ecm.platform.queue.api.QueueError;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

/**
 *
 * @author "Stephane Lacoin [aka matic] <slacoin at nuxeo.com>"
 *
 */
public class DataSerializer {

    protected XStream xstream = new XStream(new DomDriver());

    public <T> Blob toXML(T infos) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        xstream.toXML(infos, baos);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        return new InputStreamBlob(bais);
    }

    public <T> T fromXML(Blob blob, Class<T> clazz) {
        try {
            return clazz.cast(xstream.fromXML(blob.getStream()));
        } catch (IOException e) {
            throw new QueueError("Cannot read data from blob", e);
        }
    }

}
