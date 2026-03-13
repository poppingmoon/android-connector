# Module connector

Core library to subscribe and receive push notifications with UnifiedPush.

To receive notifications with UnifiedPush, users must have a dedicated application, a _distributor_, installed on their system.

This library requires Android 4.1 or higher.

## Import the library

Add the dependency to the _module_ build.gradle. Replace {VERSION} with the [latest version](https://central.sonatype.com/artifact/org.unifiedpush.android/connector).

```groovy
dependencies {
    // ...
    implementation('org.unifiedpush.android:connector:{VERSION}')
}
```

### Duplicate class errors

This library depends on `tink` which may be used by another library you depend on.
In this case, building your app may **fail because of duplicate classes**.

To resolve this conflict, add the code snippet below to your **module gradle file** (after the `plugins` and `android` blocks).
This snippet will override any version of `com.google.crypto.tink:tink` in your app's dependency graph
with the provided version of `com.google.crypto.tink:tink-android`.

<br>

> <details><summary>Gradle Kotlin DSL</summary>
>
> _app/build.gradle.kts_:
>
> ```kotlin
> configurations.configureEach {
>     val tink = "com.google.crypto.tink:tink-android:1.17.0"
>     // You can also use the library declaration catalog
>     // val tink = libs.google.tink
>     resolutionStrategy {
>         force(tink)
>         dependencySubstitution {
>             substitute(module("com.google.crypto.tink:tink")).using(module(tink))
>         }
>     }
> }
> ```
>
> </details>
>
> <details><summary>Gradle Groovy DSL</summary>
>
> _app/build.gradle_:
>
> ```groovy
> configurations.configureEach {
>     def tink = "com.google.crypto.tink:tink-android:1.17.0"
>     resolutionStrategy {
>         force(tink)
>         dependencySubstitution {
>            substitute module('com.google.crypto.tink:tink') using module(tink)
>         }
>     }
> }
> ```
>
> </details>

## Expose a Service to handle incoming events

You need to declare a service that extend [PushService][org.unifiedpush.android.connector.PushService].
This service must not be exported and must handle the action `org.unifiedpush.android.connector.PUSH_EVENT`:

```xml
<service android:name=".PushServiceImpl"
    android:exported="false">
    <intent-filter>
        <action android:name="org.unifiedpush.android.connector.PUSH_EVENT"/>
    </intent-filter>
</service>
```

## Request registrations

Interactions with the distributor are done with the [`UnifiedPush`][org.unifiedpush.android.connector.UnifiedPush] object.

<!-- Note: This must be mirrored in UnifiedPush comments -->

### Use user's default distributor

Users are allowed to define a default distributor on their system, because UnifiedPush distributors
have to be able to process a deeplink.

When you set UnifiedPush for the first time on your application, you will want to use the default user's
distributor.

Therefore, you can first use [`resolveDefaultDistributor`][org.unifiedpush.android.connector.UnifiedPush.resolveDefaultDistributor] to check if a default
distributor is defined, or if the user will have to select one.

If it resolves to [`ResolvedDistributor.ToSelect`][org.unifiedpush.android.connector.data.ResolvedDistributor.ToSelect],
the user will see an OS screen to select their distributor. We recommend introducing this OS screen
with a dialog, e.g. "You are about to select your push service".

To select the default distributor, you can use [`tryUseDefaultDistributor`][org.unifiedpush.android.connector.UnifiedPush.tryUseDefaultDistributor]
or [`tryUseCurrentOrDefaultDistributor`][org.unifiedpush.android.connector.UnifiedPush.tryUseCurrentOrDefaultDistributor] if you want
to share code for the re-registrations too. These function save the distributor to use.

When the distributor is saved, you can call [`register`][org.unifiedpush.android.connector.UnifiedPush.register] to request a new registration.
It has optional parameters, the following example uses `messageForDistributor` and `vapid`.
You can use `instance` to bring multiple-registration support to your application.

<div class="tabs">
<input class="tabs_control hidden" type="radio" id="tabs-trydefault-receiver-0" name="tabs-trydefault" checked>
<label class="tabs_label" for="tabs-trydefault-receiver-0">Kotlin</label>
<div class="tabs_content">
<!-- CONTENT KOTLIN -->

```kotlin
import org.unifiedpush.android.connector.UnifiedPush
/* ... */

UnifiedPush.tryUseCurrentOrDefaultDistributor(activityContext) { success ->
    if (success) {
        // We have a distributor
        // Register your app to the distributor
        UnifiedPush.register(context, messageForDistributor, vapid)
    }
}
```

<!-- END KOTLIN -->
</div>
<input class="tabs_control hidden" type="radio" id="tabs-trydefault-receiver-1" name="tabs-trydefault">
<label class="tabs_label" for="tabs-trydefault-receiver-1">Java</label>
<div class="tabs_content">
<!-- CONTENT JAVA -->

```java
import static org.unifiedpush.android.connector.ConstantsKt.INSTANCE_DEFAULT;
import org.unifiedpush.android.connector.UnifiedPush;
/* ... */

UnifiedPush.tryUseCurrentOrDefaultDistributor(activityContext, success ->{
    if (success) {
        // We have a distributor
        // Register your app to the distributor
        UnifiedPush.register(
            context,
            INSTANCE_DEFAULT,
            messageForDistributor,
            vapid
        );
    }
});
```

<!-- END JAVA -->
</div>
</div>

### Use another distributor

The user may want to use another distributor: it can be achieved with [`tryPickDistributor`][org.unifiedpush.android.connector.UnifiedPush.tryPickDistributor].

### Implement your own distributor picker

For this, you can get the list of available distributors with [`getDistributors`][org.unifiedpush.android.connector.UnifiedPush.getDistributors].

Once the user has chosen the distributor, you have to save it with [`saveDistributor`][org.unifiedpush.android.connector.UnifiedPush.saveDistributor].
This function must be called before [`register`][org.unifiedpush.android.connector.UnifiedPush.register].

When the distributor is saved, you can call [`register`][org.unifiedpush.android.connector.UnifiedPush.register] to request a new registration.
It has optional parameters, the following example uses `messageForDistributor` and `vapid`.
You can use `instance` to bring multiple-registration support to your application.

<div class="tabs">
<input class="tabs_control hidden" type="radio" id="tabs-1-receiver-0" name="tabs-1" checked>
<label class="tabs_label" for="tabs-1-receiver-0">Kotlin</label>
<div class="tabs_content">
<!-- CONTENT KOTLIN -->

```kotlin
import org.unifiedpush.android.connector.UnifiedPush
/* ... */

// Get a list of distributors that are available
val distributors = UnifiedPush.getDistributors(context)
// select one or ask the user which distributor to use, e.g. with a dialog
val userDistrib = yourFunc(distributors)
// save the distributor
UnifiedPush.saveDistributor(context, userDistrib)
// register your app to the distributor
UnifiedPush.register(context, messageForDistributor, vapid)
```

<!-- END KOTLIN -->
</div>
<input class="tabs_control hidden" type="radio" id="tabs-1-receiver-1" name="tabs-1">
<label class="tabs_label" for="tabs-1-receiver-1">Java</label>
<div class="tabs_content">
<!-- CONTENT JAVA -->

```java
import static org.unifiedpush.android.connector.ConstantsKt.INSTANCE_DEFAULT;
import org.unifiedpush.android.connector.UnifiedPush;
/* ... */

// Get a list of distributors that are available
List<String> distributors = UnifiedPush.getDistributors(context);
// select one or show a dialog or whatever
String userDistrib = yourFunc(distributors);
// the below line will crash the app if no distributors are available
UnifiedPush.saveDistributor(context, userDistrib);
UnifiedPush.register(
    context,
    INSTANCE_DEFAULT,
    messageForDistributor,
    vapid
);
```

<!-- END JAVA -->
</div>
</div>

### Unsubscribe

To unsubscribe, simply call [`unregister`][org.unifiedpush.android.connector.UnifiedPush.unregister]. Set the instance you want to unsubscribe to if you used one during registration.

It removes the distributor if this is the last instance to unregister.

### Regular re-registration

To ensure the distributor is still installed, and the application is correctly registered, it is recommended to regularly re-register to the distributor.

For this, from time to time, like every time the application starts, you can control if the distributor is still installed
with [`getAckDistributor`][org.unifiedpush.android.connector.UnifiedPush.getAckDistributor], then call
[`register`][org.unifiedpush.android.connector.UnifiedPush.register].

If the previous distributor is uninstalled, you can inform the user and/or fallback to the default
one again, with [`tryUseCurrentOrDefaultDistributor`][org.unifiedpush.android.connector.UnifiedPush.tryUseCurrentOrDefaultDistributor].

