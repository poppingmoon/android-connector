package org.unifiedpush.android.connector.keys

import android.content.Context
import android.os.Build
import com.google.crypto.tink.apps.fixed_webpush.WebPushHybridDecrypt
import java.security.interfaces.ECPrivateKey
import java.security.interfaces.ECPublicKey
import org.unifiedpush.android.connector.data.PublicKeySet
import org.unifiedpush.android.connector.internal.DBStore
import org.unifiedpush.android.connector.internal.keys.WebPushKeysEntries
import org.unifiedpush.android.connector.internal.keys.WebPushKeysEntries23
import org.unifiedpush.android.connector.internal.keys.WebPushKeysEntriesLegacy

/**
 * Default [KeyManager].
 *
 * For SDK >= 23, private keys are stored encrypted in shared preferences using AES-GCM, and a random
 * key in the Android Key Store.
 *
 * For SDK < 23, private keys are stored in plain text in shared preferences.
 *
 * [legacy] is used to force legacy store, for the tests
 */
class DefaultKeyManager internal constructor(private val store: DBStore.KeyStore, private val legacy: Boolean = false) : KeyManager {
    constructor(context: Context) : this(store = DBStore.get(context).keys)

    override fun decrypt(instance: String, sealed: ByteArray): ByteArray? {
        val keys = getKeyStoreEntries(instance).getWebPushKeys() ?: return null
        val hybridDecrypt =
            WebPushHybridDecrypt.Builder()
                .withAuthSecret(keys.auth)
                .withRecipientPublicKey(keys.keyPair.public as ECPublicKey)
                .withRecipientPrivateKey(keys.keyPair.private as ECPrivateKey)
                .build()
        return hybridDecrypt.decrypt(sealed, null)
    }

    override fun generate(instance: String) {
        getKeyStoreEntries(instance).genWebPushKeys()
    }

    override fun getPublicKeySet(instance: String): PublicKeySet? = getKeyStoreEntries(instance).getWebPushKeys()?.publicKeySet

    override fun exists(instance: String): Boolean = getKeyStoreEntries(instance).hasWebPushKeys()

    override fun delete(instance: String) {
        getKeyStoreEntries(instance).deleteWebPushKeys()
    }

    private fun getKeyStoreEntries(instance: String): WebPushKeysEntries = if (!legacy && Build.VERSION.SDK_INT >= 23) {
        WebPushKeysEntries23(instance, store)
    } else {
        WebPushKeysEntriesLegacy(instance, store)
    }
}
