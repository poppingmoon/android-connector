package org.unifiedpush.android.connector.internal.keys

import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Log
import androidx.annotation.RequiresApi
import org.unifiedpush.android.connector.TAG
import org.unifiedpush.android.connector.internal.DBStore
import org.unifiedpush.android.connector.internal.data.WebPushKeysRecord
import java.security.KeyFactory
import java.security.KeyPair
import java.security.KeyStore
import java.security.KeyStore.SecretKeyEntry
import java.security.interfaces.ECPublicKey
import java.security.spec.PKCS8EncodedKeySpec
import javax.crypto.AEADBadTagException
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

@RequiresApi(23)
internal class WebPushKeysEntries23(private val instance: String, private val store: DBStore.KeyStore) :
    WebPushKeysEntries {
    override fun getWebPushKeys(): WebPushKeys? {
        val record = store.get(instance) ?: run {
            Log.d(TAG, "getWebPushKeys: no record found")
            return null
        }

        val auth = record.auth.b64decode()
        val iv = record.iv?.b64decode() ?: run {
            Log.d(TAG, "getWebPushKeys: non encrypted record found")
            return null
        }
        val sealedPrivateKey = record.privKey.b64decode()
        val publicKey = record.pubKey.deserializePubKey()

        val cipher = getAesGcmCipher(iv)
        val privateBytes = try {
            cipher.doFinal(sealedPrivateKey)
        } catch (e: AEADBadTagException) {
            // If a key for a 2nd instance has been generated with a connector version
            // prior to 3.0.7, the secret key has been rotated, and would lead to
            // this exception. In this case, the key must be re-generated.
            Log.e(TAG, "AEADBadTagException caught for $instance, a new keypair must be generated.", e)
            return null
        }
        val privateKey =
            KeyFactory.getInstance("EC").generatePrivate(
                PKCS8EncodedKeySpec(privateBytes),
            )
        Log.d(TAG, "getWebPushKeys: keys found")
        return WebPushKeys(auth, KeyPair(publicKey, privateKey))
    }

    override fun genWebPushKeys(): WebPushKeys {
        Log.d(TAG, "Generating WebPushKeys")
        val keys = WebPushKeys.new()

        val cipher = getNewAesGcmCipher()
        val sealedPrivateKey = cipher.doFinal(keys.keyPair.private.encoded)
        val record = WebPushKeysRecord(
            instance,
            keys.auth.b64encode(),
            (keys.keyPair.public as ECPublicKey).serialize(),
            sealedPrivateKey.b64encode(),
            cipher.iv.b64encode()
        )
        store.set(record)
        return keys
    }

    override fun hasWebPushKeys(): Boolean {
        val ks =
            KeyStore.getInstance(KEYSTORE_PROVIDER).apply {
                load(null)
            }
        return store.get(instance)?.iv != null &&
            ks.containsAlias(ALIAS) &&
            // ks.entryInstanceOf(ALIAS, SecretKeyEntry::class.java)
            // logs an exception for some SDK, like SDK30
            // So we try to get the entry directly
            ks.getEntry(ALIAS, null) as SecretKeyEntry? != null
    }

    override fun deleteWebPushKeys() {
        Log.d(TAG, "Deleting WebPushKeys")
        store.remove(instance)
    }

    /**
     * Get secret key in AndroidKeystore for alias [ALIAS], generate it if it doesn't exist
     */
    private fun getSecretKey(): SecretKey {
        val ks =
            KeyStore.getInstance(KEYSTORE_PROVIDER).apply {
                load(null)
            }
        if (ks.containsAlias(ALIAS)) {
            return (ks.getEntry(ALIAS, null) as SecretKeyEntry).secretKey
        }
        return KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, KEYSTORE_PROVIDER).apply {
            init(
                KeyGenParameterSpec.Builder(ALIAS, KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT)
                    .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                    .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                    .build(),
            )
        }.generateKey()
    }

    /**
     * Generate new AES GCM key
     */
    private fun getNewAesGcmCipher(): Cipher {
        val secretKey = getSecretKey()
        return Cipher.getInstance("AES/GCM/NoPadding").apply {
            init(
                Cipher.ENCRYPT_MODE,
                secretKey,
            )
        }
    }

    /**
     * Get AES GCM Key for [iIV]
     */
    private fun getAesGcmCipher(iIV: ByteArray): Cipher {
        val ks =
            KeyStore.getInstance(KEYSTORE_PROVIDER).apply {
                load(null)
            }
        ks.containsAlias(ALIAS)
        val secretKey = getSecretKey()

        return Cipher.getInstance("AES/GCM/NoPadding").apply {
            init(
                Cipher.DECRYPT_MODE,
                secretKey,
                GCMParameterSpec(128, iIV),
            )
        }
    }

    companion object {
        private const val KEYSTORE_PROVIDER = "AndroidKeyStore"
        private const val ALIAS = "UnifiedPush"
    }
}
