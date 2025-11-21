package org.unifiedpush.android.connector.internal.data

/**
 * The main distributor is the one saved by the user
 *
 * The distributor in used, may be different, if the
 * main distrib has informed about a temporary migration
 * due to unavailable service.
 *
 * @param packageName is the packageName of the distributor
 * @param ack is true if we have received a new endpoint
 * after a registration
 *
 */
internal data class Distributor(
    val packageName: String,
    val ack: Boolean,
)