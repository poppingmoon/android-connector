package org.unifiedpush.android.connector.internal.keys

import org.unifiedpush.android.connector.internal.DBStore
import org.unifiedpush.android.connector.internal.data.WebPushKeysRecord
import java.security.KeyFactory
import java.security.KeyPair
import java.security.interfaces.ECPublicKey
import java.security.spec.PKCS8EncodedKeySpec

internal class WebPushKeysEntriesLegacy(private val instance: String, private val store: DBStore.KeyStore) :
    WebPushKeysEntries {
    override fun getWebPushKeys(): WebPushKeys? {
        val record = store.get(instance) ?: return null

        val auth = record.auth.b64decode()
        val privateBytes = record.privKey.b64decode()
        val publicKey = record.pubKey.deserializePubKey()

        val privateKey =
            KeyFactory.getInstance("EC").generatePrivate(
                PKCS8EncodedKeySpec(privateBytes),
            )
        return WebPushKeys(auth, KeyPair(publicKey, privateKey))
    }

    override fun genWebPushKeys(): WebPushKeys {
        val keys = WebPushKeys.new()
        val record = WebPushKeysRecord(
            instance,
            keys.auth.b64encode(),
            (keys.keyPair.public as ECPublicKey).serialize(),
            keys.keyPair.private.encoded.b64encode(),
            null
        )
        store.set(record)
        return keys
    }

    override fun hasWebPushKeys(): Boolean {
        return store.get(instance) != null
    }

    override fun deleteWebPushKeys() {
        store.remove(instance)
    }
}
