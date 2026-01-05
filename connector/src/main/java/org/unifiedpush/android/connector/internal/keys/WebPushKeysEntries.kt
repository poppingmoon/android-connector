package org.unifiedpush.android.connector.internal.keys

internal interface WebPushKeysEntries {

    fun getWebPushKeys(): WebPushKeys?

    fun genWebPushKeys(): WebPushKeys

    fun hasWebPushKeys(): Boolean

    fun deleteWebPushKeys()
}
