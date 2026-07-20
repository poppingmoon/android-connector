package org.unifiedpush.android.connector

import android.app.Activity
import android.app.BroadcastOptions
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.content.pm.ResolveInfo
import android.os.Build
import android.util.AndroidException
import android.util.Log
import org.unifiedpush.android.connector.data.ResolvedDistributor
import org.unifiedpush.android.connector.internal.DBStore
import org.unifiedpush.android.connector.internal.LinkActivity
import org.unifiedpush.android.connector.internal.data.Connection
import org.unifiedpush.android.connector.keys.DefaultKeyManager
import org.unifiedpush.android.connector.keys.KeyManager
import kotlin.jvm.Throws

/**
 * Object containing functions to interact with the distributor
 *
 * <!-- Note: This must be mirrored in Module.md -->
 *
 * ### Use user's default distributor
 *
 * Users are allowed to define a default distributor on their system, because UnifiedPush distributors
 * have to be able to process a deeplink.
 *
 * When you set UnifiedPush for the first time on your application, you will want to use the default user's
 * distributor.
 *
 * Therefore, you can first use [`resolveDefaultDistributor`][org.unifiedpush.android.connector.UnifiedPush.resolveDefaultDistributor] to check if a default
 * distributor is defined, or if the user will have to select one.
 *
 * If it resolves to [`ResolvedDistributor.ToSelect`][org.unifiedpush.android.connector.data.ResolvedDistributor.ToSelect],
 * the user will see an OS screen to select their distributor. We recommend introducing this OS screen
 * with a dialog, e.g. "You are about to select your push service".
 *
 * To select the default distributor, you can use [`tryUseDefaultDistributor`][org.unifiedpush.android.connector.UnifiedPush.tryUseDefaultDistributor]
 * or [`tryUseCurrentOrDefaultDistributor`][org.unifiedpush.android.connector.UnifiedPush.tryUseCurrentOrDefaultDistributor] if you want
 * to share code for the re-registrations too. These function save the distributor to use.
 *
 * When the distributor is saved, you can call [`register`][org.unifiedpush.android.connector.UnifiedPush.register] to request a new registration.
 * It has optional parameters, the following example uses `messageForDistributor` and `vapid`.
 * You can use `instance` to bring multiple-registration support to your application.
 *
 * <div class="tabs">
 * <input class="tabs_control hidden" type="radio" id="tabs-trydefault-receiver-0" name="tabs-trydefault" checked>
 * <label class="tabs_label" for="tabs-trydefault-receiver-0">Kotlin</label>
 * <div class="tabs_content">
 * <!-- CONTENT KOTLIN -->
 *
 * ```kotlin
 * import org.unifiedpush.android.connector.UnifiedPush
 * /* ... */
 *
 * UnifiedPush.tryUseCurrentOrDefaultDistributor(activityContext) { success ->
 *     if (success) {
 *         // We have a distributor
 *         // Register your app to the distributor
 *         UnifiedPush.register(context, messageForDistributor, vapid)
 *     }
 * }
 * ```
 *
 * <!-- END KOTLIN -->
 * </div>
 * <input class="tabs_control hidden" type="radio" id="tabs-trydefault-receiver-1" name="tabs-trydefault">
 * <label class="tabs_label" for="tabs-trydefault-receiver-1">Java</label>
 * <div class="tabs_content">
 * <!-- CONTENT JAVA -->
 *
 * ```java
 * import static org.unifiedpush.android.connector.ConstantsKt.INSTANCE_DEFAULT;
 * import org.unifiedpush.android.connector.UnifiedPush;
 * /* ... */
 *
 * UnifiedPush.tryUseCurrentOrDefaultDistributor(activityContext, success ->{
 *     if (success) {
 *         // We have a distributor
 *         // Register your app to the distributor
 *         UnifiedPush.register(
 *             context,
 *             INSTANCE_DEFAULT,
 *             messageForDistributor,
 *             vapid
 *         );
 *     }
 * });
 * ```
 *
 * <!-- END JAVA -->
 * </div>
 * </div>
 *
 * ### Use another distributor
 *
 * The user may want to use another distributor: it can be achieved with [`tryPickDistributor`][org.unifiedpush.android.connector.UnifiedPush.tryPickDistributor].
 *
 * ### Implement your own distributor picker
 *
 * For this, you can get the list of available distributors with [`getDistributors`][org.unifiedpush.android.connector.UnifiedPush.getDistributors].
 *
 * Once the user has chosen the distributor, you have to save it with [`saveDistributor`][org.unifiedpush.android.connector.UnifiedPush.saveDistributor].
 * This function must be called before [`register`][org.unifiedpush.android.connector.UnifiedPush.register].
 *
 * When the distributor is saved, you can call [`register`][org.unifiedpush.android.connector.UnifiedPush.register] to request a new registration.
 * It has optional parameters, the following example uses `messageForDistributor` and `vapid`.
 * You can use `instance` to bring multiple-registration support to your application.
 *
 * <div class="tabs">
 * <input class="tabs_control hidden" type="radio" id="tabs-1-receiver-0" name="tabs-1" checked>
 * <label class="tabs_label" for="tabs-1-receiver-0">Kotlin</label>
 * <div class="tabs_content">
 * <!-- CONTENT KOTLIN -->
 *
 * ```kotlin
 * import org.unifiedpush.android.connector.UnifiedPush
 * /* ... */
 *
 * // Get a list of distributors that are available
 * val distributors = UnifiedPush.getDistributors(context)
 * // select one or ask the user which distributor to use, e.g. with a dialog
 * val userDistrib = yourFunc(distributors)
 * // save the distributor
 * UnifiedPush.saveDistributor(context, userDistrib)
 * // register your app to the distributor
 * UnifiedPush.register(context, messageForDistributor, vapid)
 * ```
 *
 * <!-- END KOTLIN -->
 * </div>
 * <input class="tabs_control hidden" type="radio" id="tabs-1-receiver-1" name="tabs-1">
 * <label class="tabs_label" for="tabs-1-receiver-1">Java</label>
 * <div class="tabs_content">
 * <!-- CONTENT JAVA -->
 *
 * ```java
 * import static org.unifiedpush.android.connector.ConstantsKt.INSTANCE_DEFAULT;
 * import org.unifiedpush.android.connector.UnifiedPush;
 * /* ... */
 *
 * // Get a list of distributors that are available
 * List<String> distributors = UnifiedPush.getDistributors(context);
 * // select one or show a dialog or whatever
 * String userDistrib = yourFunc(distributors);
 * // the below line will crash the app if no distributors are available
 * UnifiedPush.saveDistributor(context, userDistrib);
 * UnifiedPush.register(
 *     context,
 *     INSTANCE_DEFAULT,
 *     messageForDistributor,
 *     vapid
 * );
 * ```
 *
 * <!-- END JAVA -->
 * </div>
 * </div>
 *
 * ### Unsubscribe
 *
 * To unsubscribe, simply call [`unregister`][org.unifiedpush.android.connector.UnifiedPush.unregister]. Set the instance you want to unsubscribe to if you used one during registration.
 *
 * It removes the distributor if this is the last instance to unregister.
 *
 * ### Regular re-registration
 *
 * To ensure the distributor is still installed, and the application is correctly registered, it is recommended to regularly re-register to the distributor.
 *
 * For this, from time to time, like every time the application starts, you can control if the distributor is still installed
 * with [`getAckDistributor`][org.unifiedpush.android.connector.UnifiedPush.getAckDistributor], then call
 * [`register`][org.unifiedpush.android.connector.UnifiedPush.register].
 *
 * If the previous distributor is uninstalled, you can inform the user and/or fallback to the default
 * one again, with [`tryUseCurrentOrDefaultDistributor`][org.unifiedpush.android.connector.UnifiedPush.tryUseCurrentOrDefaultDistributor].
 */
object UnifiedPush {
    // For compatibility purpose with AND_2
    private const val FEATURE_BYTES_MESSAGE = "org.unifiedpush.android.distributor.feature.BYTES_MESSAGE"

    @JvmStatic
    private val VAPID_REGEX = Regex("^[A-Za-z0-9_-]{87}$")

    /**
     * Request a new registration for the [instance] to the saved distributor.
     *
     * [saveDistributor] must be called before this function.
     *
     * If there was a distributor but it has been removed, [MessagingReceiver.onUnregistered] will be called for all subscribed instances.
     *
     * @param [context] To interact with the shared preferences and send broadcast intents.
     * @param [instance] Registration instance. Can be used to get multiple registrations, e.g. for multi-account support.
     * @param [messageForDistributor] May be shown by the distributor UI to identify this registration.
     * @param [vapid] VAPID public key ([RFC8292](https://www.rfc-editor.org/rfc/rfc8292)) base64url encoded of the uncompressed form (87 chars long).
     *
     * @throws [VapidNotValidException] if [vapid] is not in the uncompressed form and base64url encoded.
     */
    @JvmStatic
    fun register(
        context: Context,
        instance: String = INSTANCE_DEFAULT,
        messageForDistributor: String? = null,
        vapid: String? = null,
    ) {
        register(context, instance, messageForDistributor, vapid, DefaultKeyManager(context))
    }

    /** @hide */
    @Deprecated("Renamed",
        replaceWith = ReplaceWith("UnifiedPush.register(context, instance, messageForDistributor, vapid)"))
    @JvmStatic
    fun registerApp(
        context: Context,
        instance: String = INSTANCE_DEFAULT,
        messageForDistributor: String? = null,
        vapid: String? = null,
    ) = register(context, instance, messageForDistributor, vapid)

    /**
     * [register] with additional [KeyManager] parameter.
     *
     * @param [keyManager] To manage web push keys. Default: [DefaultKeyManager].
     */
    @JvmStatic
    fun register(
        context: Context,
        instance: String = INSTANCE_DEFAULT,
        messageForDistributor: String? = null,
        vapid: String? = null,
        keyManager: KeyManager,
    ) {
        val store = DBStore.get(context)
        store.registrations.set(
            instance,
            messageForDistributor,
            vapid?.replace("=", ""),
            keyManager
        ).forEach { co ->
            register(context, co)
        }
    }

    @JvmStatic
    @Throws(VapidNotValidException::class)
    internal fun register(
        context: Context,
        co: Connection.Registration,
    ) {
        co.registration.vapid?.let {
            // This is mainly to catch VAPID used with the wrong format,
            // no need to check if this is a real vapid key
            if (!VAPID_REGEX.matches(it)) {
                throw VapidNotValidException()
            }
        }

        // For SDK < 34
        val dummyIntent = Intent("org.unifiedpush.dummy_app")
        val pi = PendingIntent.getBroadcast(context, 0, dummyIntent, PendingIntent.FLAG_IMMUTABLE)

        val broadcastIntent =
            Intent().apply {
                `package` = co.distributor
                action = ACTION_REGISTER
                putExtra(EXTRA_TOKEN, co.token)
                // For compatibility with AND_2
                putExtra(EXTRA_FEATURES, arrayOf(FEATURE_BYTES_MESSAGE))
                // For compatibility with AND_2, replaced by pi for SDK < 34
                putExtra(EXTRA_APPLICATION, context.packageName)
                // For SDK < 34
                putExtra(EXTRA_PI, pi)
                co.registration.messageForDistributor?.let {
                    putExtra(EXTRA_MESSAGE_FOR_DISTRIB, it)
                }
                co.registration.vapid?.let {
                    putExtra(EXTRA_VAPID, it)
                }
            }
        if (Build.VERSION.SDK_INT >= 34) {
            val broadcastOptions = BroadcastOptions.makeBasic().setShareIdentityEnabled(true)
            context.sendBroadcast(broadcastIntent, null, broadcastOptions.toBundle())
        } else {
            context.sendBroadcast(broadcastIntent)
        }
    }

    /**
     * Send an unregistration request for the [instance] to the saved distributor and remove the registration. Remove the distributor if this is the last instance registered.
     *
     * [MessagingReceiver.onUnregistered] won't be called after that request.
     * Sends the unregistration request to the fallback distributor as well if there is one
     *
     * @param [context] To interact with the shared preferences and send broadcast intents.
     * @param [instance] Registration instance. Can be used to get multiple registrations, e.g. for multi-account support.
     */
    @JvmStatic
    fun unregister(
        context: Context,
        instance: String = INSTANCE_DEFAULT,
    ) {
        unregister(context, instance, DefaultKeyManager(context))
    }

    /** @hide */
    @Deprecated("Renamed", replaceWith = ReplaceWith("UnifiedPush.unregister(context, instance)"))
    @JvmStatic
    fun unregisterApp(
        context: Context,
        instance: String = INSTANCE_DEFAULT
    ) = unregister(context, instance)

    /**
     * [unregister] with additional [KeyManager] parameter.
     *
     * @param [keyManager] To manage web push keys. Default: [DefaultKeyManager].
     */
    @JvmStatic
    fun unregister(
        context: Context,
        instance: String = INSTANCE_DEFAULT,
        keyManager: KeyManager,
    ) {
        val store = DBStore.get(context)
        // We don't need to check if the distributor is still installed, as we delete the registration after that
        // And we send UNREGISTER to all distributors (primary/fallbacks) if we have many
        val tokens = store.registrations.listToken(instance)
        store.registrations.remove(instance, keyManager).ifEmpty {
            store.distributor.remove()
        }
        tokens.forEach {
            broadcastUnregister(context, it)
        }
    }

    internal fun broadcastUnregister(
        context: Context,
        connection: Connection.Token
    ) {

        // For SDK < 34
        val dummyIntent = Intent("org.unifiedpush.dummy_app")
        val pi = PendingIntent.getBroadcast(context, 0, dummyIntent, PendingIntent.FLAG_IMMUTABLE)

        val broadcastIntent =
            Intent().apply {
                `package` = connection.distributor
                action = ACTION_UNREGISTER
                putExtra(EXTRA_TOKEN, connection.token)
                // For SDK < 34
                putExtra(EXTRA_PI, pi)
            }

        if (Build.VERSION.SDK_INT >= 34) {
            val broadcastOptions = BroadcastOptions.makeBasic().setShareIdentityEnabled(true)
            context.sendBroadcast(broadcastIntent, null, broadcastOptions.toBundle())
        } else {
            context.sendBroadcast(broadcastIntent)
        }
    }

    /**
     * Get a list of available distributors installed on the system
     *
     * ## Embedded distributor
     *
     * If your application embeds a distributor, its package name is present in the list.
     *
     * If the embedded distributor is the `embedded_fcm_distributor` and the Play Services aren't
     * installed on the device, your application's package name is filter out.
     * **You can use it to know if the Play Services are installed**:
     * `getDistributors(ctx).contains(ctx.packageName)`
     *
     * To get the list of external distributors, you need to filter out your package name:
     * `getDistributors(ctx).filter { it != ctx.packageName }`
     *
     * @return The list of distributor's package name
     */
    @JvmStatic
    fun getDistributors(context: Context): List<String> {
        return getResolveInfo(context, ACTION_REGISTER).mapNotNull {
            // Remove local package if it has embedded fcm distrib
            // and PlayServices are not available
            if (it.activityInfo.packageName == context.packageName &&
                hasEmbeddedFcmDistributor(context) &&
                !isPlayServicesAvailable(context)
            ) {
                return@mapNotNull null
            }

            return@mapNotNull it.activityInfo.packageName.also { pn ->
                Log.d(TAG, "Found distributor with package name $pn")
            }
        }.distinct()
    }

    @JvmStatic
    internal fun getResolveInfo(
        context: Context,
        action: String,
        packageName: String? = null,
    ): List<ResolveInfo> {
        val intent =
            Intent(action).apply {
                packageName?.let {
                    `package` = it
                }
            }
        return (
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                context.packageManager.queryBroadcastReceivers(
                    intent,
                    PackageManager.ResolveInfoFlags.of(
                        PackageManager.GET_META_DATA.toLong() +
                            PackageManager.GET_RESOLVED_FILTER.toLong(),
                    ),
                )
            } else {
                context.packageManager.queryBroadcastReceivers(
                    intent,
                    PackageManager.GET_RESOLVED_FILTER,
                )
            }
        ).filter {
            it.activityInfo.exported || it.activityInfo.packageName == context.packageName
        }
    }

    /**
     * Resolve default distributor using the deeplink "unifiedpush://link"
     *
     * When the user has a default or a single distributor, it returns [ResolvedDistributor.Found],
     * and [tryUseDefaultDistributor] is transparent to the user.
     *
     * When the user has many distributors but hasn't selected a default yet, it returns
     * [ResolvedDistributor.ToSelect], and [tryUseDefaultDistributor] will open an OS screen to select one.
     *
     * Informing the user about the incoming OS screen helps them to understand why they are selecting
     * an application. The OS dialog can't have a custom title or message, and we need the OS dialog
     * to define a default distributor.
     *
     * ```
     * when (val res = UnifiedPush.resolveDefaultDistributor(this)) {
     *     is ResolvedDistributor.Found -> {
     *         // The user don't have to interact. Will be the case most of the time,
     *         // if the user has a default or a single distributor installed
     *         UnifiedPush.saveDistributor(this, res.packageName)
     *         UnifiedPush.register(this)
     *     }
     *     ResolvedDistributor.ToSelect -> {
     *         // Use your own dialog before running tryUseDefaultDistributor
     *         showDialogBeforeSelection(this)
     *     }
     *     ResolvedDistributor.NoneAvailable -> {
     *         // fallback to another solution
     *     }
     * }
     * ```
     */
    @JvmStatic
    fun resolveDefaultDistributor(context: Context): ResolvedDistributor {
        return when (val resolved = LinkActivityHelper.resolveLinkActivityPackageName(context)) {
            null -> resolveWithoutLinkActivity(context)
            "android" -> ResolvedDistributor.ToSelect
            is String -> ResolvedDistributor.Found(resolved)
        }
    }

    /**
     * Try to "resolve" embedded or legacy distributor that doesn't support LinkActivity
     */
    @JvmStatic
    private fun resolveWithoutLinkActivity(context: Context): ResolvedDistributor {
        val distributors = getDistributors(context)
        val extDistributors = distributors.filter { it != context.packageName }
        return when (true) {
            (distributors.isEmpty()) -> {
                Log.d(TAG, "No distributor available on the system")
                ResolvedDistributor.NoneAvailable
            }

            (distributors.size == 1) -> {
                // If there is a single distributor, we use it:
                // it is probably the embedded distributor
                Log.d(
                    TAG,
                    "Found a single distributor without LinkActivity: probably an embedded distrib."
                )
                ResolvedDistributor.Found(distributors.first())
            }

            (extDistributors.size == 1) -> {
                // If there is more than a single distributor,
                // then there is likely a distributor that does not implement the LinkActivity:
                // we try to use it anyway.
                // If there are more than one legacy distributor, we simply fail.
                Log.d(
                    TAG,
                    "Found a single external (legacy) distributor without LinkActivity"
                )
                ResolvedDistributor.Found(extDistributors.first())
            }

            else -> {
                Log.d(
                    TAG,
                    "Found ${extDistributors.size} external (legacy) distributors without LinkActivity, can't select one"
                )
                ResolvedDistributor.NoneAvailable
            }
        }
    }

    /**
     * Try to pick a distributor opening the deeplink "unifiedpush://link", ignoring the default one
     *
     * It uses the OS dialog to select the applications
     *
     * **This function should be called after a user interaction**, e.g. after clicking on a button.
     *
     * **External distributors will be favored over embedded distributors.**
     *
     * **Doesn't work with Distributors that don't support AND_3 specs**
     *
     * This function starts a new translucent activity in order to get the result of the
     * distributor activity.
     *
     * You may prefer to override `onActivityResult` of your own Activity and use
     * [LinkActivityHelper.startPickLinkActivityForResult] instead.
     *
     * ## Usage
     *
     * Kotlin:
     * ```
     * tryPickDistributor(activityContext) { success ->
     *     if (success) {
     *         //TODO: register, no need to saveDistributor
     *     }
     * }
     * ```
     *
     * Java:
     * ```
     * UnifiedPush.tryPickDistributor(activityContext, success -> {
     *     if (success) {
     *         //TODO: register, no need to saveDistributor
     *     }
     *     return null;
     * });
     * ```
     *
     * @param [activity] Activity Context
     * @param [callback] is a function taking a Boolean as parameter. This boolean is
     * true if the registration using the deeplink succeeded.
     */
    @JvmStatic
    fun tryPickDistributor(
        activity: Activity,
        callback: (Boolean) -> Unit,
    ) {
        tryPickDistributor(context = activity, callback)
    }

    /** @hide */
    @Deprecated(
        "Use tryPickDistributor(activity, callback) instead",
        ReplaceWith("tryPickDistributor(activity, callback)")
    )
    @JvmStatic
    fun tryPickDistributor(
        context: Context,
        callback: (Boolean) -> Unit,
    ) {
        LinkActivityHelper.resolveLinkActivityPackageName(context)?.let {
            LinkActivity.callback = callback
            val intent =
                Intent().apply {
                    setClassName(context.packageName, LinkActivity::class.java.name)
                    setAction(LinkActivityHelper.ACTION_PICK)
                }
            context.startActivity(intent)
        } ?: callback(false)
    }

    /**
     * Try to use the distributor opening the deeplink "unifiedpush://link"
     *
     * It allows users to define a default distributor for all their applications
     *
     * **Before calling this function**, you may want to resolve the default distributor with
     * [resolveDefaultDistributor], to check if the OS will show a screen to the user. In this case,
     * you can introduce this screen with a dialog "You are about to select your push service"
     *
     * **External distributors will be favored over embedded distributors.**
     *
     * This function starts a new translucent activity in order to get the result of the
     * distributor activity.
     *
     * You may prefer to override `onActivityResult` of your own Activity and use
     * [LinkActivityHelper.startLinkActivityForResult] instead.
     *
     * ## Usage
     *
     * Kotlin:
     * ```
     * tryUseDefaultDistributor(activityContext) { success ->
     *     if (success) {
     *         //TODO: register, no need to saveDistributor
     *     }
     * }
     * ```
     *
     * Java:
     * ```
     * UnifiedPush.tryUseDefaultDistributor(activityContext, success -> {
     *     if (success) {
     *         //TODO: register, no need to saveDistributor
     *     }
     *     return null;
     * });
     * ```
     *
     * @param [activity] Activity Context
     * @param [callback] is a function taking a Boolean as parameter. This boolean is
     * true if the registration using the deeplink succeeded.
     */
    @JvmStatic
    fun tryUseDefaultDistributor(
        activity: Activity,
        callback: (Boolean) -> Unit,
    ) = tryUseDefaultDistributor(context = activity, callback)

    /** @hide */
    @Deprecated(
        "Use tryUseDefaultDistributor(activity, callback) instead",
        ReplaceWith("tryUseDefaultDistributor(activity, callback)")
    )
    @JvmStatic
    fun tryUseDefaultDistributor(
        context: Context,
        callback: (Boolean) -> Unit,
    ) {
        when (val resolved = resolveDefaultDistributor(context)) {
            ResolvedDistributor.ToSelect -> {
                LinkActivity.callback = callback
                val intent =
                    Intent().apply {
                        setClassName(context.packageName, LinkActivity::class.java.name)
                    }
                context.startActivity(intent)
            }
            is ResolvedDistributor.Found -> {
                saveDistributor(context, resolved.packageName)
                callback(true)
            }
            ResolvedDistributor.NoneAvailable -> {
                callback(false)
            }
        }
    }

    /**
     * Try to use the saved distributor else, use the default distributor opening the deeplink "unifiedpush://link"
     *
     * It can be used on application startup to register to the distributor.
     * If you had already registered to a distributor, this ensures the connection is working.
     * If the previous distributor has been uninstalled, it will fall back to the user's default.
     * If you register for the first time, it will use the user's default Distributor or the OS will
     * ask what it should use.
     *
     * **Before calling this function**, you may want to check if [getAckDistributor] exists and
     * resolve the default distributor with [resolveDefaultDistributor], to check if the OS will
     * show a screen to the user. In this case, you can introduce this screen with a dialog
     * "You are about to select your push service"
     *
     * **External distributors will be favored over embedded distributors.**
     *
     * This function starts a new translucent activity in order to get the result of the
     * distributor activity.
     *
     * ## Usage
     *
     * Kotlin:
     * ```
     * tryUseCurrentOrDefaultDistributor(activityContext) { success ->
     *     if (success) {
     *         //TODO: register, no need to saveDistributor
     *     }
     * }
     * ```
     *
     * Java:
     * ```
     * UnifiedPush.tryUseCurrentOrDefaultDistributor(activityContext, success -> {
     *     if (success) {
     *         //TODO: register, no need to saveDistributor
     *     }
     *     return null;
     * });
     * ```
     *
     * @param [activity] Activity Context
     * @param [callback] is a function taking a Boolean as parameter. This boolean is
     * true if the registration using the deeplink succeeded.
     */
    @JvmStatic
    fun tryUseCurrentOrDefaultDistributor(
        activity: Activity,
        callback: (Boolean) -> Unit,
    ) =  tryUseCurrentOrDefaultDistributor(context = activity, callback)

    /** @hide */
    @Deprecated(
        "Use tryUseCurrentOrDefaultDistributor(activity, callback) instead",
        ReplaceWith("tryUseCurrentOrDefaultDistributor(activity, callback)")
    )
    @JvmStatic
    fun tryUseCurrentOrDefaultDistributor(
        context: Context,
        callback: (Boolean) -> Unit,
    ) {
        getAckDistributor(context)?.let {
            callback(true)
        } ?: run {
            tryUseDefaultDistributor(context, callback)
        }
    }

    /**
     * Save [distributor] as the new distributor to use
     *
     * @param [context] To interact with the shared preferences.
     * @param [distributor] The distributor package name
     */
    @JvmStatic
    fun saveDistributor(
        context: Context,
        distributor: String,
    ) {
        val store = DBStore.get(context)
        // We don't need to check if the distributor is still installed.
        // There is no reason saveDistributor is called with an
        // uninstalled distributor, and if in any case it is,
        // get*Distributor checks if it is installed.
        val (_, toDel) = store.distributor.setPrimary(distributor)
        toDel.forEach {
            broadcastUnregister(context, it)
        }
    }

    /**
     * Get the distributor registered by the user, but the
     * distributor may not have respond yet to our requests. Most of the time [getAckDistributor] is preferred.
     *
     * Will call [MessagingReceiver.onUnregistered] for all instances if the distributor
     * is not installed anymore.
     *
     * @return The distributor package name if any, else null
     */
    @JvmStatic
    fun getSavedDistributor(context: Context): String? = getDistributor(context, DBStore.get(context), false)

    /**
     * Get the distributor registered by the user, and the
     * distributor has already responded to our requests
     *
     * Will call [MessagingReceiver.onUnregistered] for all instances if the distributor
     * is not installed anymore.
     *
     * @return The distributor package name if any, else null
     */
    @JvmStatic
    fun getAckDistributor(context: Context): String? = getDistributor(context, DBStore.get(context), true)

    /**
     * Get the current distributor.
     *
     * Send [ACTION_UNREGISTERED] for all instances if the distributor
     * is not installed anymore.
     *
     * @param [context] [Context] to interact with package manager.
     * @param [store] [DBStore] to access our shared preferences.
     * @param [ack] `true` if the distributor has to be ack.
     */
    @JvmStatic
    private fun getDistributor(
        context: Context,
        store: DBStore,
        ack: Boolean,
    ): String? {
        val savedDistributors = store.distributor.list()
        val availableDistributors = getDistributors(context)
        return if (savedDistributors.isEmpty()) {
            null
        } else if (savedDistributors.none { it.packageName in availableDistributors}) {
            Log.d(TAG, "There was a distributor, but it isn't installed anymore")
            store.registrations.listToken(null).forEach { t ->
                broadcastLocalUnregistered(context, t.token)
            }
            null
        } else {
            savedDistributors.filter { it.packageName !in availableDistributors }
                .forEach {
                store.distributor.remove(it.packageName)
            }
            store.distributor.get().takeIf { !ack || it?.ack == true }?.packageName
        }
    }

    @JvmStatic
    private fun broadcastLocalUnregistered(
        context: Context,
        token: String
    ) {
        val broadcastIntent =
            Intent().apply {
                `package` = context.packageName
                action = ACTION_UNREGISTERED
                putExtra(EXTRA_TOKEN, token)
            }
        context.sendBroadcast(broadcastIntent)
    }

    @JvmStatic
    private fun hasEmbeddedFcmDistributor(context: Context): Boolean {
        val packageInfo =
            context.packageManager.getPackageInfo(
                context.packageName,
                PackageManager.GET_SERVICES + PackageManager.GET_RECEIVERS,
            )
        val serviceNames = packageInfo.services?.map { it.name } ?: emptyList()
        val receiverNames = packageInfo.receivers?.map { it.name } ?: emptyList()
        return (serviceNames + receiverNames).any { s ->
            s.startsWith("org.unifiedpush.android.embedded_fcm_distributor.") ||
                    s.startsWith("org.unifiedpush.android.foss_embedded_fcm_distributor.")
        }
    }

    @JvmStatic
    private fun isPlayServicesAvailable(context: Context): Boolean {
        val pm = context.packageManager
        try {
            pm.getPackageInfo("com.google.android.gms", PackageManager.GET_ACTIVITIES)
            return true
        } catch (e: PackageManager.NameNotFoundException) {
            Log.v(TAG, "Google services not found: ${e.message}")
        }
        return false
    }

    /**
     * Unregister all instances and remove the distributor
     *
     * @param [context] To interact with the shared preferences and send broadcast intents.
     */
    @JvmStatic
    fun removeDistributor(context: Context) {
        removeDistributor(context, DefaultKeyManager(context))
    }

    /** @hide */
    @Deprecated("Renamed", replaceWith = ReplaceWith("UnifiedPush.removeDistributor(context)"))
    @JvmStatic
    fun forceRemoveDistributor(context: Context) = removeDistributor(context)

    /**
     * [removeDistributor] with additional [KeyManager] parameter.
     *
     * @param [keyManager] To manage web push keys. Default: [DefaultKeyManager].
     */
    @JvmStatic
    fun removeDistributor(
        context: Context,
        keyManager: KeyManager,
    ) {
        val store = DBStore.get(context)
        store.registrations.listInstances().forEach {
            unregister(context, it, keyManager)
        }
        store.registrations.removeAll(keyManager)
        store.distributor.remove()
    }

    /**
     * The VAPID public key is not in the right format.
     *
     * It should be in the uncompressed form, and base64url encoded (87 chars long)
     */
    class VapidNotValidException : AndroidException()
}
