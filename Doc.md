# UnifiedPush libraries for Android apps

The following libraries are provided by the [UnifiedPush](https://unifiedpush.org) project to make it easy for developers to integrate UnifiedPush into their applications for receiving push notifications.

To learn more, see the high-level developer documentation:

- <https://unifiedpush.org/developers/intro/>
- <https://unifiedpush.org/developers/ux/>

## Core Library (connector)

The <a href="./connector">connector</a> library is the core library for handling subscribing to a distributor app, receiving push notifications, and more.
This is the library you should start with when integrating UnifiedPush.

## Embedded FCM Distributor

The <a href="./embedded_fcm_distributor">embedded_fcm_distributor</a> library adds a fallback to Google's Firebase Cloud Messaging (FCM).
This is useful for users who don't have a UnifiedPush distributor app installed, but who have Play Services on their device.

It acts like an UnifiedPush distributor, but it is internal to your app and passes notifications through FCM.
It doesn't contain proprietary code.

## Example implementation

The [example application](https://codeberg.org/UnifiedPush/android-example) shows the basic usage of these libraries.
