package org.unifiedpush.android.connector.internal.keys

import android.util.Log
import org.unifiedpush.android.connector.TAG

internal interface WebPushKeysEntries {

    fun getWebPushKeys(): WebPushKeys?

    fun genWebPushKeys(): WebPushKeys

    fun hasWebPushKeys(): Boolean

    fun deleteWebPushKeys()
}
