package org.unifiedpush.android.connector.internal.data

/**
 * Internal object for a registration
 */
internal data class RegistrationData(
    val instance: String,
    val messageForDistributor: String?,
    val vapid: String?,
)