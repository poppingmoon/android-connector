package org.unifiedpush.android.connector.internal.data

/**
 * Internal object for a [Registration]
 */
internal data class Registration(
    val instance: String,
    val token: String,
    val messageForDistributor: String?,
    val vapid: String?,
)