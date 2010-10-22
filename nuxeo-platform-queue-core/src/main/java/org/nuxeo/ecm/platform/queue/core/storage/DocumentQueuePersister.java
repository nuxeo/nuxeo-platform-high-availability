/*
 * (C) Copyright 2010 Nuxeo SA (http://nuxeo.com/) and contributors.
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
 *     Sun Seng David TAN (a.k.a. sunix) <stan@nuxeo.com>
 */
package org.nuxeo.ecm.platform.queue.core.storage;

import static org.nuxeo.ecm.platform.queue.core.storage.DocumentQueueConstants.QUEUEITEM_BLACKLIST_TIME;
import static org.nuxeo.ecm.platform.queue.core.storage.DocumentQueueConstants.QUEUEITEM_CONTENT_PROPERTY;
import static org.nuxeo.ecm.platform.queue.core.storage.DocumentQueueConstants.QUEUEITEM_ERROR_PROPERTY;
import static org.nuxeo.ecm.platform.queue.core.storage.DocumentQueueConstants.QUEUEITEM_EXECUTE_TIME;
import static org.nuxeo.ecm.platform.queue.core.storage.DocumentQueueConstants.QUEUEITEM_EXECUTION_COUNT_PROPERTY;
import static org.nuxeo.ecm.platform.queue.core.storage.DocumentQueueConstants.QUEUEITEM_OWNER;
import static org.nuxeo.ecm.platform.queue.core.storage.DocumentQueueConstants.QUEUEITEM_SCHEMA;
import static org.nuxeo.ecm.platform.queue.core.storage.DocumentQueueConstants.QUEUEITEM_SERVERID;
import static org.nuxeo.ecm.platform.queue.core.storage.DocumentQueueConstants.QUEUE_ITEM_TYPE;
import static org.nuxeo.ecm.platform.queue.core.storage.DocumentQueueConstants.QUEUE_ROOT_NAME;
import static org.nuxeo.ecm.platform.queue.core.storage.DocumentQueueConstants.QUEUE_ROOT_TYPE;
import static org.nuxeo.ecm.platform.queue.core.storage.DocumentQueueConstants.QUEUE_TYPE;

import java.io.Serializable;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentRef;
import org.nuxeo.ecm.core.api.IdRef;
import org.nuxeo.ecm.core.api.IterableQueryResult;
import org.nuxeo.ecm.core.api.PathRef;
import org.nuxeo.ecm.core.api.impl.DocumentModelImpl;
import org.nuxeo.ecm.core.management.storage.DocumentStoreManager;
import org.nuxeo.ecm.core.query.sql.NXQL;
import org.nuxeo.ecm.platform.heartbeat.api.HeartbeatManager;
import org.nuxeo.ecm.platform.queue.api.QueueError;
import org.nuxeo.ecm.platform.queue.api.QueueInfo;
import org.nuxeo.ecm.platform.queue.api.QueuePersister;
import org.nuxeo.runtime.api.Framework;

/**
 * Persist a queue inside a nuxeo repository as a folder and files
 *
 * @author Sun Seng David TAN (a.k.a. sunix) <stan@nuxeo.com>
 */
public class DocumentQueuePersister<C extends Serializable> extends StorageManager implements QueuePersister<C> {

    public static final Log log = LogFactory.getLog(DocumentQueuePersister.class);

    protected PathRef rootPath() {
        return DocumentStoreManager.newPath(QUEUE_ROOT_NAME);
    }

    protected PathRef queuePath() {
        return DocumentStoreManager.newPath(QUEUE_ROOT_NAME, queueName);
    }

    protected DocumentModel queue(CoreSession session) throws ClientException {
        return session.getDocument(queuePath());
    }

    protected final String queueName;

    protected final Class<C> contentType;

    public DocumentQueuePersister(String queueName, Class<C> contentType) {
        this.queueName = queueName;
        this.contentType = contentType;
    }

    @Override
    public void createIfNotExist() {
        try {
            PathRef queuePath = queuePath();
            if (session.exists(queuePath)) {
                return;
            }
            PathRef rootPath = rootPath();
            DocumentModel root;
            if (!session.exists(rootPath)) {
                root = session.createDocumentModel(DocumentStoreManager.newPath().toString(), QUEUE_ROOT_NAME, QUEUE_ROOT_TYPE);
                root = session.createDocument(root);
            } else {
                root = session.getDocument(rootPath);
            }
            DocumentModel queue = session.createDocumentModel(rootPath.toString(), queueName, QUEUE_TYPE);
            queue = session.createDocument(queue);
            session.save();
        } catch (ClientException e) {
            throw new QueueError("Cannot setup queue", e);
        }
    }

    @Override
    public QueueInfo<C> removeContent(URI contentName) {
        try {
            DocumentModel doc;
            PathRef ref = newPathRef(session, contentName);
            doc = session.getDocument(ref);
            detachDocument(doc);
            session.removeDocument(ref);
            session.save();
            return new DocumentQueueAdapter<C>(doc, contentType);
        } catch (ClientException e) {
            throw new QueueError("Cannot remove content for " + contentName, e);
        }
    }


    @Override
    public boolean hasContent(URI name) {
        try {
        PathRef ref = newPathRef(session, name);
        return session.exists(ref);
        } catch (ClientException e) {
            throw new QueueError("Cannot test content for " + name, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<QueueInfo<C>> listKnownItems() {
        try {
        DocumentModel queueDoc = queue(session);
        List<DocumentModel> docs = session.getChildren(queueDoc.getRef());
        List<QueueInfo<C>> infos = new ArrayList<QueueInfo<C>>(docs.size());
        for (DocumentModel doc:docs) {
            detachDocument(doc);
            infos.add(doc.getAdapter(QueueInfo.class));
        }
        return infos;
        } catch (ClientException e) {
            throw new QueueError("Cannot list content of " + queueName, e);
        }
    }

      @Override
    public QueueInfo<C> addContent(URI ownerName, URI name,  C content)  {
        try {
            HeartbeatManager heartbeat = Framework.getLocalService(HeartbeatManager.class);
            PathRef ref = newPathRef(session, name);
            if (session.exists(ref)) {
                throw new QueueError("Already created queue item", name);
            }

            DocumentModel parent = queue(session);
            DocumentModel doc = session.createDocumentModel(parent.getPathAsString(), name.toASCIIString(), QUEUE_ITEM_TYPE);

            doc.setProperty(QUEUEITEM_SCHEMA, QUEUEITEM_OWNER, ownerName.toASCIIString());
            doc.setProperty(QUEUEITEM_SCHEMA, QUEUEITEM_SERVERID, heartbeat.getInfo().getId().toASCIIString());

            injectData(doc, QUEUEITEM_CONTENT_PROPERTY, content);

            doc = session.createDocument(doc);
            detachDocument(doc);
            session.save();
            return new DocumentQueueAdapter<C>(doc, contentType);
        } catch (ClientException e) {
            throw new QueueError("Cannot add content " + name, e);
        }
    }

    @Override
    public QueueInfo<C> setLaunched(URI contentName) {
        try {
            PathRef ref = newPathRef(session, contentName);
            DocumentModel doc = session.getDocument(ref);
            Long executionCount = (Long) doc.getPropertyValue(QUEUEITEM_EXECUTION_COUNT_PROPERTY);
            if (executionCount == null) {
                executionCount = 1L;
            } else {
                executionCount++;
            }
            doc.setProperty(QUEUEITEM_SCHEMA, QUEUEITEM_EXECUTE_TIME, new Date());
            doc.setPropertyValue(QUEUEITEM_EXECUTION_COUNT_PROPERTY, executionCount);
            doc = session.saveDocument(doc);
            detachDocument(doc);
            session.save();
            return new DocumentQueueAdapter<C>(doc, contentType);
        } catch (ClientException e) {
            throw new QueueError("Cannot set launched for " + contentName);
        }
    }

    @Override
    public QueueInfo<C> setBlacklisted(URI contentName) {
        try {
            PathRef ref = newPathRef(session, contentName);
            DocumentModel doc = session.getDocument(ref);
            doc.setProperty(QUEUEITEM_SCHEMA, QUEUEITEM_BLACKLIST_TIME, new Date());
            doc = session.saveDocument(doc);
            detachDocument(doc);
            session.save();
            return new DocumentQueueAdapter<C>(doc, contentType);
        } catch (ClientException e) {
            throw new QueueError("Cannot blacklist " + contentName, e);
        }
    }

    @Override
    public void updateContent(URI contentName, C content) {
        try {
            PathRef ref = newPathRef(session, contentName);
            DocumentModel doc = session.getDocument(ref);
            injectData(doc, QUEUEITEM_CONTENT_PROPERTY, content);
            doc = session.saveDocument(doc);
            session.save();
        } catch (ClientException e) {
            throw new QueueError("Cannot update content for " + contentName, e);
        }
    }

    @Override
    public List<QueueInfo<C>> listByOwner(URI ownerName) {
        try {
        DocumentModel queue = queue(session);
        String query = String.format("SELECT * FROM QueueItem WHERE ecm:parentId = '%s' AND  qitm:owner = '%s'",
                queue.getId(),
                ownerName.toASCIIString());
        List<DocumentModel> docs = session.query(query);
        List<QueueInfo<C>> infos = new ArrayList<QueueInfo<C>>(docs.size());
        for (DocumentModel doc:docs) {
            detachDocument(doc);
            infos.add(new DocumentQueueAdapter<C>(doc, contentType));
        }
        return infos;
        } catch (ClientException e) {
            throw new QueueError("Cannot list content for " + ownerName);
        }
    }


    @Override
    public int removeByOwner(URI ownerName) {
        try {
            DocumentModel queue = queue(session);
            String query = String.format("SELECT ecm:uuid FROM QueueItem WHERE ecm:parentId = '%s' AND  qitm:owner = '%s'",
                    queue.getId(),
                    ownerName.toASCIIString());
            return doRemove(query);
        } catch (ClientException e) {
            throw new QueueError("Cannot remove content owned by " + ownerName, e);
        }
    }

    @Override
    public QueueInfo<C> getInfo(URI contentName) {
        try {
            DocumentModel queue = queue(session);
            DocumentModel doc = session.getChild(queue.getRef(), contentName.toASCIIString());
            if (doc == null) {
                throw new QueueError("no such content", contentName);
            }
            detachDocument(doc);
            return new DocumentQueueAdapter<C>(doc, contentType);
        } catch (ClientException e) {
            throw new QueueError("Canot get info of " + contentName, e);
        }
    }

    protected static String formatTimestamp(Date date) {
        return new SimpleDateFormat("'TIMESTAMP' ''yyyy-MM-dd HH:mm:ss.SSS''").format(date);
    }

    protected int doRemove(String query) throws ClientException {
        IterableQueryResult res = session.queryAndFetch(query, "NXQL");
        try {
            int removedCount = 0;
            for (Map<String, Serializable> map : res) {
                DocumentRef ref = new IdRef((String) map.get(NXQL.ECM_UUID));
                session.removeDocument(ref);
                session.save();
                removedCount += 1;
            }
            return removedCount;
        } finally {
            res.close();
        }
    }

    @Override
    public int removeBlacklisted(URI queueName, Date from) {
        try {
            String ts = formatTimestamp(from);
            log.debug("Removing blacklisted doc oldest than " + ts + " for " + queueName);
            String query = String.format("SELECT ecm:uuid FROM QueueItem WHERE ecm:path STARTSWITH '%s' AND qitm:blacklistTime < %s AND ecm:isProxy = 0", queuePath(), ts);
            return doRemove(query);
        } catch (ClientException e) {
            throw new QueueError("Cannot remove blacklisted content of " + queueName, e);
        }
    }

    protected void injectData(DocumentModel doc, String xpath, Serializable content) throws ClientException {
        Blob blob = null;
        if (content != null) {
            blob =  new DataSerializer().toXML(content);
        }
        doc.setPropertyValue(xpath, (Serializable)blob);
    }

    protected  PathRef newPathRef(CoreSession session, URI name) throws ClientException {
        DocumentModel queueFolder = queue(session);
        return new PathRef(queueFolder.getPathAsString() + "/" + name.toASCIIString());
    }

    protected static void detachDocument(DocumentModel doc) throws ClientException {
        ((DocumentModelImpl) doc).detach(true);
    }

       @Override
    public QueueInfo<C> saveError(URI name,  Throwable error)  {
        try {
            DocumentModel queue = queue(session);
            DocumentModel doc = session.getChild(queue.getRef(), name.toASCIIString());
            if (doc == null) {
                throw new QueueError("no such content", name);
            }

            injectData(doc, QUEUEITEM_ERROR_PROPERTY, error);

            doc = session.saveDocument(doc);
            detachDocument(doc);
            session.save();

            return new DocumentQueueAdapter<C>(doc, contentType);
        } catch (ClientException e) {
            throw new QueueError("Cannot save error for " + name, e);
        }
    }

              @Override
    public QueueInfo<C> cancelError(URI contentName)  {
        try {
            DocumentModel queue = queue(session);
            DocumentModel doc = session.getChild(queue.getRef(), contentName.toASCIIString());
            if (doc == null) {
                throw new QueueError("no such content", contentName);
            }

            HeartbeatManager heartbeat = Framework.getLocalService(HeartbeatManager.class);

            doc.setProperty(QUEUEITEM_SCHEMA, QUEUEITEM_SERVERID, heartbeat.getInfo().getId().toASCIIString());
            doc.setPropertyValue(QUEUEITEM_ERROR_PROPERTY, null);


            doc = session.saveDocument(doc);
            detachDocument(doc);
            session.save();

            return new DocumentQueueAdapter<C>(doc, contentType);
        } catch (ClientException e) {
            throw new QueueError("Cannot reset " + contentName, e);
        }
    }

}
