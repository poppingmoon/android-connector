package org.unifiedpush.android.connector.internal

import android.content.Context
import android.content.SharedPreferences
import org.unifiedpush.android.connector.PREF_CONNECTOR_MESSAGE
import org.unifiedpush.android.connector.PREF_CONNECTOR_TOKEN
import org.unifiedpush.android.connector.PREF_CONNECTOR_VAPID
import org.unifiedpush.android.connector.PREF_MASTER
import org.unifiedpush.android.connector.PREF_MASTER_DISTRIBUTOR
import org.unifiedpush.android.connector.PREF_MASTER_DISTRIBUTOR_ACK
import org.unifiedpush.android.connector.PREF_MASTER_INSTANCES

internal class LegacyStore(context: Context) {
    private var preferences: SharedPreferences = context
        .getSharedPreferences(PREF_MASTER, Context.MODE_PRIVATE)

    /**
     * Migrate distributor from SHARED_PREFS
     *
     * @param block if returns `true`: remove the distributor
     */
    fun migrateDistributor(block: (Distributor) -> Boolean) {
        val packageName = preferences.getString(PREF_MASTER_DISTRIBUTOR, null)
        val ack = preferences.getBoolean(PREF_MASTER_DISTRIBUTOR_ACK, false)
        if (packageName == null ||
            block(Distributor(packageName, ack, null, null))
            ) {
            preferences
                .edit()
                .remove(PREF_MASTER_DISTRIBUTOR)
                .remove(PREF_MASTER_DISTRIBUTOR_ACK)
                .apply()
        }
    }

    /**
     * Migrate registrations from SHARED_PREFS
     *
     * @param block if returns `true`: remove the registration
     */
    fun migrateRegistrations(block: (Registration) -> Boolean) {
        var failed = false
        preferences.getStringSet(PREF_MASTER_INSTANCES, null)?.forEach { instance ->
            val token = preferences.getString(PREF_CONNECTOR_TOKEN.format(instance), null)
            val message = preferences.getString(PREF_CONNECTOR_MESSAGE.format(instance), null)
            val vapid = preferences.getString(PREF_CONNECTOR_VAPID.format(instance), null)
            if (token == null ||
                block(Registration(instance, token, message, vapid))
                ) {
                preferences.edit()
                    .remove(PREF_CONNECTOR_TOKEN.format(instance))
                    .remove(PREF_CONNECTOR_VAPID.format(instance))
                    .remove(PREF_CONNECTOR_MESSAGE.format(instance))
                    .apply()
            } else {
                failed = true
            }
        }
        if (!failed) {
            preferences.edit().remove(PREF_MASTER_INSTANCES).apply()
        }
    }
}
