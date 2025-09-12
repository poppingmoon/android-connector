package org.unifiedpush.android.connector.internal.data

internal sealed class Connection {
    data class Token(
        val distributor: String,
        val token: String
    )
    data class Registration(
        val distributor: String,
        val token: String,
        val registration: RegistrationData
    )
    data class Instance(
        val distributor: String,
        val instance: String
    )
}