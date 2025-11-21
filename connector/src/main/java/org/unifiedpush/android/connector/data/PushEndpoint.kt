package org.unifiedpush.android.connector.data

import android.os.Parcel
import android.os.Parcelable

/**
 * Contains the push endpoint and the associated [PublicKeySet].
 */
class PushEndpoint(
    /** URL to push notifications to. */
    val url: String,
    /** Web Push public key set. */
    val pubKeySet: PublicKeySet?,
    /**
     * The endpoint may change soon: it comes from another distributor used until
     * the primary one is back online
     *
     * Distributors are able to tell applications to use another service when their backend
     * is unavailable. This field allows you to ignore the temporary fallback if you want to.
     */
    val temporary: Boolean,
) : Parcelable {
    override fun writeToParcel(
        parcel: Parcel,
        flags: Int,
    ) {
        parcel.writeInt(if (temporary) 1 else 0)
        parcel.writeString(url)
        parcel.writeInt(pubKeySet?.let { 1 } ?: 0)
        pubKeySet?.writeToParcel(parcel, flags)
    }

    override fun describeContents(): Int {
        return 0
    }

    companion object CREATOR : Parcelable.Creator<PushEndpoint> {
        override fun createFromParcel(parcel: Parcel): PushEndpoint? {
            val temporary = parcel.readInt() == 1
            val url = parcel.readString()
            val pubKeySet =
                if (parcel.readInt() == 1) {
                    PublicKeySet.createFromParcel(parcel)
                } else {
                    null
                }
            return PushEndpoint(
                url ?: return null,
                pubKeySet,
                temporary,
            )
        }

        override fun newArray(size: Int): Array<PushEndpoint?> {
            return arrayOfNulls(size)
        }
    }
}
