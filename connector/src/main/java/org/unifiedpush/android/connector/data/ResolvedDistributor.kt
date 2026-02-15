package org.unifiedpush.android.connector.data

/**
 * Result of default distributor resolver
 *
 * Uses UnifiedPush Link Activity deep link
 */
sealed class ResolvedDistributor {
    /**
     * @param packageName is the default distributor on the system
     */
    data class Found(val packageName: String): ResolvedDistributor()

    /**
     * There are multiple distributors available, none of them is the default yet.
     *
     * The system will show a dialog when trying to use the default distributor
     */
    object ToSelect: ResolvedDistributor()

    /**
     * The user doesn't have any distributor
     */
    object NoneAvailable: ResolvedDistributor()
}