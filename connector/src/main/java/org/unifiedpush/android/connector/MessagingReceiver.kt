package org.unifiedpush.android.connector

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.util.Log
import org.unifiedpush.android.connector.data.PushEndpoint
import org.unifiedpush.android.connector.data.PushMessage
import org.unifiedpush.android.connector.internal.DBStore
import org.unifiedpush.android.connector.internal.WakeLock
import org.unifiedpush.android.connector.keys.DefaultKeyManager
import org.unifiedpush.android.connector.keys.KeyManager
import java.security.GeneralSecurityException

/**
 * **Deprecated**, please use [PushService] instead.
 *
 * Receive UnifiedPush messages (new endpoints, unregistrations, push messages, errors) from the distributors
 *
 * ## Deprecation note
 * The library already embed a receiver implementing this receiver and forward the events to a service. This
 * allow us to maintain the declaration of the exposed service, which can make maintenance easier if any change
 * is required in the future.
 *
 * It is still possible to use this receiver directly: if a receiver with this intent filter is declared
 * in your manifest, the one declared in the library won't run.
 *
 * ## Expose this receiver
 *
 * The receiver has to be exposed in the `AndroidManifest.xml` in order to receive the UnifiedPush messages.
 *
 * ```xml
 * <receiver android:exported="true"  android:enabled="true"  android:name=".CustomReceiver">
 *     <intent-filter>
 *         <action android:name="org.unifiedpush.android.connector.MESSAGE"/>
 *         <action android:name="org.unifiedpush.android.connector.UNREGISTERED"/>
 *         <action android:name="org.unifiedpush.android.connector.NEW_ENDPOINT"/>
 *         <action android:name="org.unifiedpush.android.connector.REGISTRATION_FAILED"/>
 *         <action android:name="org.unifiedpush.android.connector.TEMP_UNAVAILABLE"/>
 *     </intent-filter>
 * </receiver>
 * ```
 */
abstract class MessagingReceiver : BroadcastReceiver() {
    /**
     * Define the [KeyManager] to use. [DefaultKeyManager] by default.
     *
     * If you wish to change the [KeyManager], you need to call [UnifiedPush.register],
     * [UnifiedPush.unregister] and [UnifiedPush.removeDistributor] with the same
     * KeyManager.
     *
     * @return a [KeyManager]
     */
    open fun getKeyManager(context: Context): KeyManager {
        return DefaultKeyManager(context)
    }

    /**
     * A new endpoint is to be used for sending push messages. The new endpoint
     * should be send to the application server.
     */
    abstract fun onNewEndpoint(
        context: Context,
        endpoint: PushEndpoint,
        instance: String,
    )

    /**
     * The registration is not possible, eg. no network, depending on the reason,
     * you can try to register again directly.
     */
    abstract fun onRegistrationFailed(
        context: Context,
        reason: FailedReason,
        instance: String,
    )

    /**
     * This registration is unregistered by the distributor and won't receive push messages anymore.
     *
     * The registration should be removed from the application server.
     */
    abstract fun onUnregistered(
        context: Context,
        instance: String,
    )

    /**
     * The distributor backend is temporary unavailable.
     *
     * A fallback solution can be implemented until the push server is back online.
     * For example, it is possible to implement an internal connection to the application
     * server or periodically fetch notifications to the application server.
     * When the server is available again, [onNewEndpoint] is called.
     *
     * Does nothing by default
     */
    open fun onTempUnavailable(
        context: Context,
        instance: String,
    ) {}

    /**
     * A new message is received. The message contains the decrypted content of the push message
     * for the instance
     */
    abstract fun onMessage(
        context: Context,
        message: PushMessage,
        instance: String,
    )

    /**
     * Handle UnifiedPush messages, should not be override
     */
    override fun onReceive(
        context: Context,
        intent: Intent,
    ) {
        val token = intent.getStringExtra(EXTRA_TOKEN)
        val store = DBStore.get(context)
        val keyManager = getKeyManager(context)
        val co = token?.let {
            store.registrations.getInstance(it)
        } ?: return
        val wakeLock = WakeLock(context)
        when (intent.action) {
            ACTION_NEW_ENDPOINT -> {
                val endpoint = intent.getStringExtra(EXTRA_ENDPOINT) ?: return
                val id = intent.getStringExtra(EXTRA_MESSAGE_ID)
                val pubKeys = keyManager.getPublicKeySet(co.instance)
                store.distributor.ack(co.distributor)
                    .forEach {
                        // If there were some fallbacks, they are now removed
                        // And we can inform them now
                        UnifiedPush.broadcastUnregister(context, it)
                    }
                val primary = store.distributor.isPrimary(co.distributor)
                onNewEndpoint(
                    context,
                    PushEndpoint(endpoint, pubKeys, !primary),
                    co.instance
                )
                mayAcknowledgeMessage(context, co.distributor, id, token)
            }
            ACTION_REGISTRATION_FAILED -> {
                val reason = intent.getStringExtra(EXTRA_REASON).toFailedReason()
                Log.i(TAG, "Failed: $reason")
                onRegistrationFailed(context, reason, co.instance)
            }
            ACTION_TEMP_UNAVAILABLE -> {
                intent.getStringExtra(EXTRA_NEW_DISTRIBUTOR)?.let { distrib ->
                    try {
                        val (new, toDel) = store.distributor.setFallback(co.distributor, distrib)
                        toDel.forEach { co ->
                            // Broadcast UNREGISTER to potentially removed fallbacks
                            UnifiedPush.broadcastUnregister(context, co)
                        }
                        if (new) {
                            store.registrations.list().forEach { co ->
                                store.registrations.set(
                                    co.instance,
                                    co.messageForDistributor,
                                    co.vapid,
                                    keyManager
                                ).filter {
                                    it.distributor == distrib
                                }.forEach { co ->
                                    UnifiedPush.register(context, co)
                                }
                            }
                        }
                        true
                    } catch (_: DBStore.CyclicFallbackException) {
                        null
                    }
                } ?: run {
                    onTempUnavailable(context, co.instance)
                }
            }
            ACTION_UNREGISTERED -> {
                intent.getStringExtra(EXTRA_NEW_DISTRIBUTOR)?.let { distrib ->
                    val (new, toDel) = store.distributor.setPrimary(distrib)
                    toDel.forEach { co ->
                        // Broadcast UNREGISTER to potentially removed fallbacks
                        UnifiedPush.broadcastUnregister(context, co)
                    }
                    if (new) {
                        store.registrations.list().forEach { r ->
                            UnifiedPush.register(
                                context,
                                r.instance,
                                r.messageForDistributor,
                                r.vapid,
                                getKeyManager(context)
                            )
                        }
                    }
                } ?: run {
                    // When we receive UNREGISTERED from any distributor, it means the registration
                    // have to be removed  for all of the fallbacks, and primary too
                    // so we send UNREGISTER to all (pot. fallback) distrib
                    // This is fine to send UNREGISTER to the distributor that send UNREGISTERED,
                    // it should simply ignore the request.
                    UnifiedPush.unregister(context, co.instance, keyManager)
                    onUnregistered(context, co.instance)
                }
            }
            ACTION_MESSAGE -> {
                val message = intent.getByteArrayExtra(EXTRA_BYTES_MESSAGE) ?: return
                val id = intent.getStringExtra(EXTRA_MESSAGE_ID)
                val pushMessage =
                    try {
                        keyManager.decrypt(co.instance, message)?.let {
                            PushMessage(it, true)
                        } ?: PushMessage(message, false)
                    } catch (e: GeneralSecurityException) {
                        Log.w(TAG, "Could not decrypt message, trying with plain text. Cause: ${e.message}")
                        PushMessage(message, false)
                    }
                onMessage(context, pushMessage, co.instance)
                mayAcknowledgeMessage(context, co.distributor, id, token)
            }
        }
        wakeLock.release()
    }

    private fun mayAcknowledgeMessage(
        context: Context,
        distributor: String,
        id: String?,
        token: String,
    ) {
        id?.let {
            val broadcastIntent =
                Intent().apply {
                    `package` = distributor
                    action = ACTION_MESSAGE_ACK
                    putExtra(EXTRA_TOKEN, token)
                    putExtra(EXTRA_MESSAGE_ID, it)
                }
            context.sendBroadcast(broadcastIntent)
        }
    }
}
