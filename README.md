# UnifiedPush android-connector

This library enables end user applications to easily integrate UnifiedPush.
This library communicates with distributor apps and handles things such as registration, unregistration, and receiving push messages.

More formally, this library implements the end user application part of the [UnifiedPush Android specification](https://unifiedpush.org/developers/spec/android/) (so you don't have to).

## Documentation

- The [example application](https://codeberg.org/UnifiedPush/android-example) shows the basic usage of this library.
- The library documentation is available at <https://unifiedpush.org/kdoc/>.
- General documentation is available at <https://unifiedpush.org>.

## Building the documentation

Documentation for this library can be generated with [Dokka](https://kotlinlang.org/docs/dokka-introduction.html):

```console
./gradlew dokkaHtmlMultiModule
```

The result is available in the `kdoc/` directory.

The [scripts/doc.sh](scripts/doc.sh) automates building the documentation for all UnifiedPush libraries.

```console
./scripts/doc.sh
```

## Funding

This project is funded through [NGI Zero Core](https://nlnet.nl/core), a fund established by [NLnet](https://nlnet.nl) with financial support from the European Commission's [Next Generation Internet](https://ngi.eu) program. Learn more at the [NLnet project page](https://nlnet.nl/project/UnifiedPush).

[<img src="https://codeberg.org/UnifiedPush/documentation/raw/branch/main/static/img/nlnet_banner.png" alt="NLnet foundation logo" width="20%" />](https://nlnet.nl)
[<img src="https://codeberg.org/UnifiedPush/documentation/raw/branch/main/static/img/NGI0_tag.svg" alt="NGI Zero Logo" width="20%" />](https://nlnet.nl/core)
