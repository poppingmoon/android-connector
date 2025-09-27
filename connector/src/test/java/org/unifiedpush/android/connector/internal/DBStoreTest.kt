package org.unifiedpush.android.connector.internal

import androidx.test.ext.junit.runners.AndroidJUnit4
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RuntimeEnvironment

@RunWith(AndroidJUnit4::class)
class DBStoreTest {

    private lateinit var db: DBStore

    @Before
    fun setup() {
        val context = RuntimeEnvironment.getApplication()
        db = DBStore(context)
    }

    @Test
    fun primaryDistrib() {
        assertNoDistrib()
        db.distributor.setPrimary(DISTRIB_1)
        assertPrimary(DISTRIB_1)
        assertDistrib(DISTRIB_1, false)
        db.distributor.ack(DISTRIB_1)
        assertDistrib(DISTRIB_1, true)
        db.distributor.remove(DISTRIB_1)
        assertNoDistrib()
    }

    /**
     * From D1
     * To D2
     * with setPrimary()
     */
    @Test
    fun replacePrimaryDistrib() {
        assertNoDistrib()
        setD1()
        // Replace
        assertContainsOnlyTokenFor(listOf(DISTRIB_1)) {
            val res = db.distributor.setPrimary(DISTRIB_2)
            assert(res.first)
            res.second.map { it.token }
        }
        assertPrimary(DISTRIB_2)
        assertDistrib(DISTRIB_2, false)
        // We can try to remove the old one, nothing changes
        db.distributor.remove(DISTRIB_1)
        assertPrimary(DISTRIB_2)
        assertDistrib(DISTRIB_2, false)
        db.distributor.remove()
    }

    /**
     * From D1
     * To D1
     * with setPrimary()
     *
     * Should not do anything
     */
    @Test
    fun replaceSamePrimaryDistrib() {
        assertNoDistrib()
        setD1()
        db.distributor.ack(DISTRIB_1)
        // Replace
        assertContainsOnlyTokenFor(listOf()) {
            val res = db.distributor.setPrimary(DISTRIB_1)
            assert(!res.first)
            res.second.map { it.token }
        }
        assertPrimary(DISTRIB_1)
        assertDistrib(DISTRIB_1, true)

        db.distributor.remove()
    }

    /**
     * From D1 -> D2
     * To D3
     * with setPrimary()
     */
    @Test
    fun replacePrimaryDistribWithFallbacks() {
        assertNoDistrib()
        setD1D2()

        // Replace
        assertContainsOnlyTokenFor(listOf(DISTRIB_1, DISTRIB_2)) {
            val res = db.distributor.setPrimary(DISTRIB_3)
            assert(res.first)
            res.second.map { it.token }
        }
        assertPrimary(DISTRIB_3)
        assertDistrib(DISTRIB_3, false)
        assertKnowOnlyDistribs(listOf(DISTRIB_3))

        // We can try to remove the old one, nothing changes
        db.distributor.remove(DISTRIB_1)
        assertDistrib(DISTRIB_3, false)

        db.distributor.remove()
    }

    /**
     * From D1 -> D2 -> D3
     * To D2 -> D3
     * with setPrimary()
     */
    @Test
    fun makeFallbackDistribPrimary() {
        assertNoDistrib()
        setD1D2D3()

        // Replace
        db.distributor.setPrimary(DISTRIB_2)
        assertPrimary(DISTRIB_2)
        assertDistrib(DISTRIB_3, true)
        assertKnowOnlyDistribs(listOf(DISTRIB_2, DISTRIB_3))

        // We can try to remove the old one, nothing changes
        db.distributor.remove(DISTRIB_1)
        assertDistrib(DISTRIB_3, true)

        db.distributor.remove()
    }

    /**
     * From D1 -> D2 -> D3
     * To D1 -> D2
     *   And
     * From D1 -> D2 -> D3
     * To D1
     * with ack()
     */
    @Test
    fun fallbackDistribAck() {
        assertNoDistrib()
        setD1D2D3()

        // Ack fallback, should remove the 2nd but not primary
        db.distributor.ack(DISTRIB_2)
        assertPrimary(DISTRIB_1)
        assertDistrib(DISTRIB_2, true)
        assertKnowOnlyDistribs(listOf(DISTRIB_1, DISTRIB_2))

        // Add again DISTRIB_3 to test ack primary
        db.distributor.setFallback(DISTRIB_2, DISTRIB_3)
        assertPrimary(DISTRIB_1)
        assertDistrib(DISTRIB_3, false)

        // Now Ack primary, should remove the 2 fallbacks
        db.distributor.ack(DISTRIB_1)
        assertPrimary(DISTRIB_1)
        assertDistrib(DISTRIB_1, true)
        assertKnowOnlyDistribs(listOf(DISTRIB_1))

        db.distributor.remove()
        assertEquals(null, db.distributor.get())
    }

    /**
     * From D1 -> D2 -> D3
     * To D1 -> D3
     * with setFallback()
     */
    @Test
    fun fallbackDistribReplaceFallback() {
        assertNoDistrib()
        setD1D2D3()

        db.distributor.setFallback(DISTRIB_1, DISTRIB_3)
        assertPrimary(DISTRIB_1)
        assertDistrib(DISTRIB_3, true)
        assertKnowOnlyDistribs(listOf(DISTRIB_1, DISTRIB_3))
    }

    /**
     * From D1 -> D2 -> D3
     * Try D1 -> D2 -> D3 -> D2
     * Get D1 -> D2 -> D3 to avoid cyclic dep
     * with setFallback()
     */
    @Test
    fun fallbackDistribCyclic() {
        assertNoDistrib()
        setD1D2D3()
        assertThrows(DBStore.CyclicFallbackException::class.java) {
            db.distributor.setFallback(DISTRIB_3, DISTRIB_2)
        }
        assertPrimary(DISTRIB_1)
        assertDistrib(DISTRIB_3, true)
    }

    /**
     * From D1 -> D2 -> D3
     * To D2 -> D3
     * With remove()
     */
    @Test
    fun fallbackDistribRemoveFirst() {
        assertNoDistrib()
        setD1D2D3()
        db.distributor.remove(DISTRIB_1)
        assertKnowOnlyDistribs(listOf(DISTRIB_2, DISTRIB_3))
        assertPrimary(DISTRIB_2)
        assertDistrib(DISTRIB_3, true)
    }

    /**
     * From D1 -> D2 -> D3
     * To D1 -> D3
     * With remove()
     */
    @Test
    fun fallbackDistribRemoveMiddle() {
        assertNoDistrib()
        setD1D2D3()
        db.distributor.remove(DISTRIB_2)
        assertKnowOnlyDistribs(listOf(DISTRIB_1, DISTRIB_3))
        assertPrimary(DISTRIB_1)
        assertDistrib(DISTRIB_3, true)
    }

    /**
     * From D1 -> D2 -> D3
     * To D1 -> D2
     * With remove()
     */
    @Test
    fun fallbackDistribRemoveLast() {
        assertNoDistrib()
        setD1D2D3()
        db.distributor.remove(DISTRIB_3)
        assertKnowOnlyDistribs(listOf(DISTRIB_1, DISTRIB_2))
        assertPrimary(DISTRIB_1)
        assertDistrib(DISTRIB_2, true)
    }

    fun addRegistration() {
        db.registrations.set(
            REGISTRATION_INSTANCE,
            REGISTRATION_MESSAGE,
            REGISTRATION_VAPID,
            null
        )
    }

    /**
     * Initial: D1 (ack: false)
     */
    fun setD1() {
        // Create primary
        db.distributor.setPrimary(DISTRIB_1)
        assertPrimary(DISTRIB_1)
        assertDistrib(DISTRIB_1, false)
        assertKnowOnlyDistribs(listOf(DISTRIB_1))
        addRegistration()
        assert(db.registrations.getToken(REGISTRATION_INSTANCE, DISTRIB_1) != null)
    }

    /**
     * Initial: D1 (ack: false) -> D2 (ack: true)
     */
    fun setD1D2() {
        setD1()
        // Set fallback
        db.distributor.setFallback(DISTRIB_1, DISTRIB_2)
        assertPrimary(DISTRIB_1)
        assertDistrib(DISTRIB_2, false)
        // Ack fallback distrib
        db.distributor.ack(DISTRIB_2)
        assertPrimary(DISTRIB_1)
        assertDistrib(DISTRIB_2, true)
        assertKnowOnlyDistribs(listOf(DISTRIB_1, DISTRIB_2))
        // We need to call addRegistration again, because after setFallback, we
        // call UnifiedPush.register for all instances
        addRegistration()
        assert(db.registrations.getToken(REGISTRATION_INSTANCE, DISTRIB_2) != null)
    }

    /**
     * Initial: D1 (ack: false) -> D2 (ack: true) -> D3 (ack: true)
     */
    fun setD1D2D3() {
        setD1D2()
        // Set 2nd fallback
        db.distributor.setFallback(DISTRIB_2, DISTRIB_3)
        db.distributor.ack(DISTRIB_3)
        assertPrimary(DISTRIB_1)
        assertDistrib(DISTRIB_3, true)
        assertKnowOnlyDistribs(listOf(DISTRIB_1, DISTRIB_2, DISTRIB_3))
        // We need to call addRegistration again, because after setFallback, we
        // call UnifiedPush.register for all instances
        addRegistration()
        assert(db.registrations.getToken(REGISTRATION_INSTANCE, DISTRIB_3) != null)
    }

    fun assertPrimary(distrib: String) {
        assert(db.distributor.isPrimary(distrib))
        distribs
            .filter { it != distrib }
            .forEach { d ->
            assert(!db.distributor.isPrimary(d))
        }
    }

    fun assertDistrib(packageName: String, ack: Boolean) {
        assertEquals(packageName, db.distributor.get()?.packageName)
        assertEquals(ack, db.distributor.get()?.ack)
    }

    fun assertNoDistrib() {
        assertEquals(null, db.distributor.get())
    }

    fun assertKnowOnlyDistribs(distribs: List<String>) {
        val list = db.distributor.list().map { it.packageName }
        val unknownList = distribs.toMutableSet()
        distribs.forEach { d ->
            assert(list.contains(d)) { "$d not in $list"}
            unknownList.remove(d)
        }
        unknownList.forEach { d ->
            assert(!list.contains(d)) { "$d in $list"}
        }
    }

    /**
     * @param block returns a set of tokens
     */
    fun assertContainsOnlyTokenFor(distribs: List<String>, block: () -> List<String>) {
        val unknownList = distribs.toMutableSet()
        val tokens = distribs.map { d ->
            unknownList.remove(d)
            db.registrations.getToken(REGISTRATION_INSTANCE, d)
        }
        val potRemovedTokens = unknownList.mapNotNull { d ->
            db.registrations.getToken(REGISTRATION_INSTANCE, d)
        }
        val res = block()
        tokens.forEach {
            assert(it != null)
            assert(res.contains(it)) { "$it not in $res" }
        }
        potRemovedTokens.forEach {
            assert(!res.contains(it))
        }
        unknownList.forEach { d ->
            assertEquals(null, db.registrations.getToken(REGISTRATION_INSTANCE, d))
        }
    }

    @After
    fun tearDown() {
        db.distributor.remove()
        db.close()
    }

    companion object {
        const val DISTRIB_1 = "distrib.1"
        const val DISTRIB_2 = "distrib.2"
        const val DISTRIB_3 = "distrib.3"
        const val REGISTRATION_INSTANCE = "instance_"
        const val REGISTRATION_MESSAGE = "message_"
        const val REGISTRATION_VAPID = "BA1Hxzyi1RUM1b5wjxsn7nGxAszw2u61m164i3MrAIxHF6YK5h4SDYic-dRuU_RCPCfA5aq9ojSwk5Y2EmClBPs"
        val distribs = setOf(
            DISTRIB_1, DISTRIB_2, DISTRIB_3
        )
    }
}