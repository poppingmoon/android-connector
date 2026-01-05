#!/bin/sh -e

NORMAL=$'\e[0m'
GREEN=$(tput setaf 2)
RED=$(tput setaf 1)

AGE_IDENTITY="$HOME/.passage/identities"
ENC_TOKEN_FILE="$HOME/.passage/store/mavenCentral/token.age"

if [ -z $OCT_CARD ]; then
    echo "[!] OCT_CARD not set, aborting"
    exit 1
fi

## Read details

ARTIFACT_ID=$( ./gradlew printArtifactId | sed -n '/^artifact=/ s/.*=//p' )
RELEASE_VERSION=$( ./gradlew printVersion | sed -n '/^version=/ s/.*=//p' )
DIR="$( echo "${ARTIFACT_ID:?}" | sed 's-[.:]-/-g' )/${RELEASE_VERSION:?}"

## Confirm

echo "You are going to release ${GREEN}${ARTIFACT_ID:?}${NORMAL}:${RED}${RELEASE_VERSION:?}${NORMAL}"
echo "Type ${RED}${RELEASE_VERSION}${NORMAL} to confirm:"
read CONFIRMATION
if [ "${RELEASE_VERSION:?}" != "${CONFIRMATION}" ]; then
    echo "Aborting."
    exit 1
fi
echo "Confirmed."

## Get token

if [ ! -f $ENC_TOKEN_FILE ]; then
    echo "[!] $ENC_TOKEN_FILE not found. Aborting."
    exit 1
fi
echo "Decrypting $ENC_TOKEN_FILE"
TOKEN=$(rage -i $AGE_IDENTITY -d $ENC_TOKEN_FILE)

if [ $? -ne 0 ]; then
    echo "An error occurred while decrypting the token. Aborting."
    exit 1
fi

## Build

rm -rf ~/.m2/repository/${DIR:?}
./gradlew publishReleasePublicationToMavenLocal

## Sign

cd ~/.m2/repository/
FILES=$(find ${DIR:?} -type f)
N_FILES=$(echo "$FILES" | wc -l)
if [ $N_FILES -le 0 ]; then
    echo "[!] No file to sign, aborting."
    exit 1
fi
echo "[+] Signing $N_FILES files"
oct batch sign --card "$OCT_CARD" detached -o asc $FILES
for file in $FILES; do
    sha1sum $file | sed 's/ .*//' > $file.sha1
    md5sum $file | sed 's/ .*//' > $file.md5
done

## Zip

ZIP_FILE="${ARTIFACT_ID:?}:${RELEASE_VERSION:?}.zip "

zip -r ${ZIP_FILE:?} ${DIR:?}/

## Upload

echo "Do you want to upload ${ZIP_FILE} ? Type ${RED}yes${NORMAL}"

read CONFIRMATION

if [ "$CONFIRMATION" = "yes" ]; then
    curl --request POST \
        --header "Authorization: Bearer ${TOKEN:?}" \
        --form bundle=@${ZIP_FILE:?} \
        https://central.sonatype.com/api/v1/publisher/upload
fi
