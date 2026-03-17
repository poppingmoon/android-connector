#!/bin/sh

# Helper script to build the documentation of all UnifiedPush libraries

# Clone the additional repositories we want to build
git clone --filter=blob:none https://codeberg.org/UnifiedPush/android-connector-ui
ln -s android-connector-ui/connector_ui .

git clone --filter=blob:none https://codeberg.org/UnifiedPush/android-embedded_fcm_distributor
ln -s android-embedded_fcm_distributor/embedded_fcm_distributor .

echo """
include ':connector_ui'
include ':embedded_fcm_distributor'
""" >> settings.gradle

echo ""
echo "If you want, you can now 'cd' to the other repositories and check out a specific git ref at which you want to build the documentation."
echo "Once you are happy, enter [y] to continue and to build the documentation."

read CONFIRMATION
if [ "${CONFIRMATION}" != "y" ]; then
    echo "Aborting."
    exit 0
fi

./gradlew dokkaHtmlMultiModule
