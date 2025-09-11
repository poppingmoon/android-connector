package org.unifiedpush.android.connector.internal.data

/**
 * Record to save and retrieve [org.unifiedpush.android.connector.internal.keys.WebPushKeys] in the db
 */
internal data class WebPushKeysRecord(
    val instance: String,
    val auth: String,
    val pubKey: String,
    /**
     * May be encrypted
     */
    val privKey: String,
    /**
     * Used to decrypt [privKey] on SDK 23+
     */
    val iv: String?
)
