package org.unifiedpush.android.connector.internal

import android.content.ContentValues
import android.content.Context
import android.database.Cursor
import android.database.sqlite.SQLiteConstraintException
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper
import android.util.AndroidException
import android.util.Log
import org.unifiedpush.android.connector.TAG
import org.unifiedpush.android.connector.internal.data.Distributor
import org.unifiedpush.android.connector.internal.data.RegistrationData
import org.unifiedpush.android.connector.internal.data.WebPushKeysRecord
import org.unifiedpush.android.connector.internal.data.Connection
import org.unifiedpush.android.connector.keys.KeyManager
import java.util.UUID

internal class DBStore(context: Context) :
    SQLiteOpenHelper(context, DB_NAME, null, DB_VERSION) {

    val distributor = DistributorStore()
    val registrations = RegistrationsStore()
    val keys = KeyStore()

    override fun onCreate(db: SQLiteDatabase) {
        Log.d(TAG, "Creating $DB_NAME")
        db.execSQL(CREATE_TABLE_DISTRIBUTORS)
        db.execSQL(CREATE_TABLE_REGISTRATIONS)
        db.execSQL(CREATE_TABLE_TOKENS)
        db.execSQL(CREATE_TABLE_KEYS)
        // onUpgrade(db, 1, DB_VERSION)
    }

    override fun onUpgrade(
        db: SQLiteDatabase,
        oldVersion: Int,
        newVersion: Int
    ) {
        Log.d(TAG, "Upgrading $DB_NAME from $oldVersion to $newVersion")
        if (oldVersion >= newVersion) throw IllegalStateException("Upgrade not supported")
        // TODO uncomment onUpgrade in onCreate
        /*
        var v = oldVersion
        while (v < newVersion) {
            when (v) {
                1 -> db.execSQL(UPGRADE_1_2)
                else -> throw IllegalStateException("Upgrade not supported")
            }
            v++
        }
        */
    }

    override fun onConfigure(db: SQLiteDatabase) {
        super.onConfigure(db)
        db.setForeignKeyConstraintsEnabled(true)
    }

    fun migrateFromLegacy(context: Context) {
        val store = LegacyStore(context)
        store.migrateDistributor {
            distributor.setPrimary(it.packageName)
            if (it.ack) distributor.ack(it.packageName)
            return@migrateDistributor true
        }
        val distrib = distributor.get()
        store.migrateRegistrations(distrib?.packageName) { co ->
            registrations.set(
                co.registration.instance,
                co.registration.messageForDistributor,
                co.registration.vapid,
                null,
                setOf(co.coToken())
            )
            store.migrateWebPushKeysRecord(co.registration.instance) { rec ->
                keys.set(rec)
                return@migrateWebPushKeysRecord true
            }
            return@migrateRegistrations true
        }
    }

    inner class DistributorStore() {

        /**
         * Change primary distributor, remove the previous one and its fallback distributors.
         *
         * The connection tokens for the previous distributor(s) are
         * wiped,
         *
         * Does nothing if the distributor is already saved as a primary distrib
         *
         * If the distributor is known as a fallback one, update it to make it primary
         *
         * If this is a new distributor, it isn't acknowledged yet and don't have
         * any fallback.
         *
         * @return `(new, toDel)`:
         *   * `new`: whether this is a new distributor
         *   * `toDel` is a set of removed [Connection.Token], so it is possible to send
         *   UNREGISTER to them
         */
        fun setPrimary(distributor: String): Pair<Boolean, Set<Connection.Token>> {
            /**
             * We used to do that, but this isn't necessary anymore
             * ```
             *         if (store.tryGetDistributor() != distributor) {
             *             store.distributorAck = false
             *             store.saveDistributor(distributor)
             *         }
             * ```
             */
            val db = writableDatabase
            var fallbacks = emptySet<Connection.Token>()
            var exists = false
            db.runTransaction {
                val projection = arrayOf(FIELD_FALLBACK_FROM)
                var selection = "$FIELD_DISTRIBUTOR = ?"
                val selectionArgs = arrayOf(distributor)
                exists = db.query(
                    TABLE_DISTRIBUTORS,
                    projection,
                    selection,
                    selectionArgs,
                    null,
                    null,
                    null
                ).use {
                    it.moveToFirst()
                }

                if (exists) {
                    // 1.A. Update the fallback distrib to be a primary distrib (fallback_from = NULL)
                    val values = ContentValues().apply {
                        putNull(FIELD_FALLBACK_FROM)
                    }
                    val selection = "$FIELD_DISTRIBUTOR = ?"
                    val selectionArgs = arrayOf(distributor)
                    db.update(TABLE_DISTRIBUTORS, values, selection, selectionArgs)

                } else {
                    // 1.B. Insert the new distrib
                    val values = ContentValues().apply {
                        put(FIELD_DISTRIBUTOR, distributor)
                        put(FIELD_ACK, 0)
                        putNull(FIELD_FALLBACK_FROM)
                        putNull(FIELD_FALLBACK_TO)
                    }
                    db.insertWithOnConflict(
                        TABLE_DISTRIBUTORS,
                        null,
                        values,
                        SQLiteDatabase.CONFLICT_REPLACE
                    )
                }

                // 2. Remove the previous primary distrib and its fallbacks (with the cascade)
                selection = "$FIELD_DISTRIBUTOR != ? AND $FIELD_FALLBACK_FROM is NULL"
                fallbacks = getFallbackToChain(selection, selectionArgs)
                db.delete(TABLE_DISTRIBUTORS, selection, selectionArgs)
            }
            return Pair(!exists, fallbacks)
        }

        /**
         * Set fallback distributor.
         *
         * If a fallback distrib isn't used anymore, remove it and its fallback distributors.
         * The connection tokens for the previous distributor(s) are
         * wiped.
         *
         * Does nothing if the distributor is already saved as a primary or fallback distrib
         *
         * @return `(new, toDel)`:
         *   * `new`: whether this is a new distributor
         *   * `toDel` is a set of removed [Connection.Token], so it is possible to send
         *   UNREGISTER to them
         *
         * @throws [CyclicFallbackException] if [from] is already a fallback of [to]
         */
        fun setFallback(from: String, to: String): Pair<Boolean, Set<Connection.Token>> {
            /**
             *  With this chain D1 -> D2 -> D3
             *  setFallback(D3, D2) => must be ignored to avoid cyclic fallback chain
             *  setFallback(D1, D3) => OK
             */
            val db = writableDatabase
            var fallbacks = emptySet<Connection.Token>()
            var toExists = false
            db.runTransaction {
                var selection = "$FIELD_DISTRIBUTOR = ?"
                var selectionArgs = arrayOf(to)

                // We ignore the fallback if `to` already fallback on `from`, to avoid cyclic fallback
                if (getFallbackToChain(
                        selection,
                        selectionArgs,
                        db
                    ).any { it.distributor == from }) {
                    throw CyclicFallbackException()
                }


                val projection = arrayOf(FIELD_FALLBACK_FROM)
                val fromExists = db.query(
                    TABLE_DISTRIBUTORS,
                    projection,
                    selection,
                    arrayOf(from),
                    null,
                    null,
                    null
                ).use {
                    it.moveToFirst()
                }

                if (!fromExists) {
                    // We abort, this should not be called with this from
                    return@runTransaction
                }

                toExists = db.query(
                    TABLE_DISTRIBUTORS,
                    projection,
                    selection,
                    arrayOf(to),
                    null,
                    null,
                    null
                ).use {
                    it.moveToFirst()
                }

                // We ignore the fallback if we already rely on it, to avoid cyclic fallback
                if (toExists) {
                    // 1.A. Change `to.fallback_from`
                    selection = "$FIELD_DISTRIBUTOR = ?"
                    selectionArgs = arrayOf(to)
                    val values = ContentValues().apply {
                        put(FIELD_FALLBACK_FROM, from)
                    }
                    db.update(
                        TABLE_DISTRIBUTORS,
                        values,
                        selection,
                        selectionArgs
                    )
                } else {
                    // 1.B. Add `to`
                    val values = ContentValues().apply {
                        put(FIELD_DISTRIBUTOR, to)
                        put(FIELD_ACK, 0)
                        put(FIELD_FALLBACK_FROM, from)
                        putNull(FIELD_FALLBACK_TO)
                    }
                    db.insertWithOnConflict(
                        TABLE_DISTRIBUTORS,
                        null,
                        values,
                        SQLiteDatabase.CONFLICT_REPLACE
                    )
                }
                // 2. Change `from.fallback_to`
                selection = "$FIELD_DISTRIBUTOR = ?"
                selectionArgs = arrayOf(from)
                val values = ContentValues().apply {
                    put(FIELD_FALLBACK_TO, to)
                }
                db.update(
                    TABLE_DISTRIBUTORS,
                    values,
                    selection,
                    selectionArgs
                )

                // 3. Remove the previous distrib that was falling back from the same from
                // and its fallbacks (with the cascade)
                selection = "$FIELD_DISTRIBUTOR != ? AND $FIELD_FALLBACK_FROM = ?"
                selectionArgs = arrayOf(to, from)
                fallbacks = getFallbackToChain(selection, selectionArgs)
                db.delete(TABLE_DISTRIBUTORS, selection, selectionArgs)
            }
            return Pair(!toExists, fallbacks)
        }

        /**
         * Try to get distributor in use. May be different
         * from the saved distributor
         */
        fun get(): Distributor? {
            val db = readableDatabase
            // We need all columns
            val projection = null
            val selection = "$FIELD_FALLBACK_TO IS NULL"
            return db.query(
                TABLE_DISTRIBUTORS,
                projection,
                selection,
                null,
                null,
                null,
                null
            ).use {
                it.moveToFirst() || return@use null
                it.distributor()
            }
        }

        /**
         * Is the distributor the primary one (not a temp fallback)
         */
        fun isPrimary(distributor: String): Boolean {
            val db = readableDatabase
            val projection = arrayOf(FIELD_DISTRIBUTOR)
            val selection = "$FIELD_DISTRIBUTOR = ? AND $FIELD_FALLBACK_FROM IS NULL"
            val selectionArgs = arrayOf(distributor)
            return db.query(
                TABLE_DISTRIBUTORS,
                projection,
                selection,
                selectionArgs,
                null,
                null,
                null
            ).use {
                it.moveToFirst()
            }
        }

        /**
         * Acknowledge [distributor] and remove its fallbacks
         *
         * Set distributor ack to true
         *
         * @return the set of removed distributor and their registration token
         */
        fun ack(distributor: String): Set<Connection.Token> {
            val db = writableDatabase
            var fallbacks = emptySet<Connection.Token>()

            db.runTransaction {
                // 1. set ack = true
                var selection = "$FIELD_DISTRIBUTOR = ?"
                val selectionArgs = arrayOf(distributor)
                val values = ContentValues().apply {
                    put(FIELD_ACK, 1)
                }
                db.update(TABLE_DISTRIBUTORS, values, selection, selectionArgs)

                // 2. get list of fallbacks connections
                fallbacks = getFallbackToChain(selection, selectionArgs, db)

                // 3. Delete fallbacks
                //     - the cascade will delete next fallbacks, and associated tokens
                //     - fallback_to will be set to null
                selection = "$FIELD_FALLBACK_FROM = ?"
                db.delete(TABLE_DISTRIBUTORS, selection, selectionArgs)
            }

            return fallbacks
        }

        /**
         * Get all `fallback_to` from [originSelection]
         *
         * @return a set of [Connection.Token] for the fallback connections
         */
        fun getFallbackToChain(
            originSelection: String,
            originSelectionArgs: Array<String>,
            db: SQLiteDatabase = writableDatabase
        ) : Set<Connection.Token> {
            val query =
                "WITH RECURSIVE rec(origin, $FIELD_DISTRIBUTOR, $FIELD_FALLBACK_TO) AS (" +
                        "SELECT 1, $FIELD_DISTRIBUTOR, $FIELD_FALLBACK_TO" +
                        " FROM $TABLE_DISTRIBUTORS" +
                        " WHERE %s".format(originSelection) +
                        " UNION ALL" +
                        " SELECT 0, t.$FIELD_DISTRIBUTOR, t.$FIELD_FALLBACK_TO" +
                        " FROM $TABLE_DISTRIBUTORS t" +
                        " JOIN rec ON t.$FIELD_DISTRIBUTOR = rec.$FIELD_FALLBACK_TO" +
                        "   AND t.$FIELD_FALLBACK_FROM = rec.$FIELD_DISTRIBUTOR" +
                        ") " +
                        " SELECT rec.$FIELD_DISTRIBUTOR, t.$FIELD_CONNECTOR_TOKEN" +
                        " FROM rec" +
                        " INNER JOIN $TABLE_TOKENS t" +
                        " ON rec.$FIELD_DISTRIBUTOR = t.$FIELD_DISTRIBUTOR" +
                        " WHERE origin = 0"
            return db.rawQuery(query, originSelectionArgs)
                .use {
                    val distribColumn = it.getColumnIndex(FIELD_DISTRIBUTOR)
                    val tokenColumn = it.getColumnIndex(FIELD_CONNECTOR_TOKEN)
                    if (distribColumn < 0 || tokenColumn < 0) return@use emptySet()
                    generateSequence {
                        if (it.moveToNext()) it else null
                    }.map { r ->
                        Connection.Token(
                            r.getString(distribColumn),
                            r.getString(tokenColumn)
                        )
                    }.toSet()
                }
        }

        /**
         * Remove the primary distributor and fallbacks if there
         * are any
         */
        fun remove() {
            val db = writableDatabase
            db.delete(TABLE_DISTRIBUTORS, null, null)
        }

        /**
         * Remove [distributor] and fix the fallback chain
         */
        fun remove(distributor: String) {
            val db = writableDatabase
            db.runTransaction {
                // 1. change fallback_to of the previous distrib
                var query = "UPDATE $TABLE_DISTRIBUTORS" +
                        " SET $FIELD_FALLBACK_TO = (" +
                        "   SELECT $FIELD_FALLBACK_TO" +
                        "   FROM $TABLE_DISTRIBUTORS" +
                        "   WHERE $FIELD_DISTRIBUTOR = ?" +
                        ")" +
                        " WHERE $FIELD_FALLBACK_TO = ?"
                var selectionArgs = arrayOf(distributor, distributor)
                // 2. change fallback_from of the next distrib
                db.rawQuery(query, selectionArgs).close()
                query = "UPDATE $TABLE_DISTRIBUTORS" +
                        " SET $FIELD_FALLBACK_FROM = (" +
                        "   SELECT $FIELD_FALLBACK_FROM" +
                        "   FROM $TABLE_DISTRIBUTORS" +
                        "   WHERE $FIELD_DISTRIBUTOR = ?" +
                        ")" +
                        " WHERE $FIELD_FALLBACK_FROM = ?"
                db.rawQuery(query, selectionArgs).close()
                // 3. delete distributor
                val selection = "$FIELD_DISTRIBUTOR = ?"
                selectionArgs = arrayOf(distributor)
                db.delete(TABLE_DISTRIBUTORS, selection, selectionArgs)
            }
        }

        /**
         * List all distributors
         */
        fun list(): Set<Distributor> {
            val db = readableDatabase
            return db.query(TABLE_DISTRIBUTORS, null, null, null, null, null, null)
                .use {
                    generateSequence {
                        if (it.moveToNext()) it else null
                    }.mapNotNull { c ->
                        c.distributor()
                    }.toSet()
                }
        }


        /**
         * Get [Distributor] from a query cursor
         */
        private fun Cursor.distributor(): Distributor? {
            val distribColumn = getColumnIndex(FIELD_DISTRIBUTOR)
            val ackColumn = getColumnIndex(FIELD_ACK)

            val packageName = (
                    if (distribColumn >= 0) getString(distribColumn) else null
                    ) ?: return null
            val ack = if (ackColumn >= 0) getInt(ackColumn) != 0 else false

            return Distributor(packageName, ack)
        }
    }

    inner class RegistrationsStore() {

        private fun genToken(): String = UUID.randomUUID().toString()

        /**
         * Try to get the connection token for [instance] and [distributor]
         */
        fun getToken(
            instance: String,
            distributor: String,
            db: SQLiteDatabase = readableDatabase
        ): String? {
            val projection = arrayOf(FIELD_CONNECTOR_TOKEN)
            val selection = "$FIELD_INSTANCE = ? AND $FIELD_DISTRIBUTOR = ?"
            val selectionArgs = arrayOf(instance, distributor)
            return db.query(
                TABLE_TOKENS,
                projection,
                selection,
                selectionArgs,
                null,
                null,
                null
            ).use {
                val col = it.getColumnIndex(FIELD_CONNECTOR_TOKEN)
                if (it.moveToFirst() && col >= 0) {
                    it.getString(col)
                } else {
                    null
                }
            }
        }

        /**
         * Generate new connection token for [instance] and [distributor]
         *
         * The token may be updated if an error occurred
         *
         * @return the potentially updated value of the token
         */
        fun newToken(
            instance: String,
            distributor: String,
            db: SQLiteDatabase = writableDatabase
        ): String {
            val token = genToken()
            val values = ContentValues().apply {
                put(FIELD_INSTANCE, instance)
                put(FIELD_DISTRIBUTOR, distributor)
                put(FIELD_CONNECTOR_TOKEN, token)
            }
            try {
                db.insertWithOnConflict(
                    TABLE_TOKENS,
                    null,
                    values,
                    SQLiteDatabase.CONFLICT_FAIL
                )
                return token
            } catch (_: SQLiteConstraintException) {
                return newToken(instance, distributor, db)
            }
        }

        /**
         * Save connection token for [instance] and [distributor]
         *
         * Used during the migration from shared prefs.
         */
        private fun saveToken(
            instance: String,
            distributor: String,
            token: String,
            db: SQLiteDatabase = writableDatabase
        ) {
            val values = ContentValues().apply {
                put(FIELD_INSTANCE, instance)
                put(FIELD_DISTRIBUTOR, distributor)
                put(FIELD_CONNECTOR_TOKEN, token)
            }
            db.insertWithOnConflict(
                TABLE_TOKENS,
                null,
                values,
                SQLiteDatabase.CONFLICT_REPLACE
            )
        }

        /**
         * Try to get the instance from the [connectionToken]
         */
        fun getInstance(connectionToken: String): Connection.Instance? {
            val db = readableDatabase
            val projection = arrayOf(FIELD_INSTANCE, FIELD_DISTRIBUTOR)
            val selection = "$FIELD_CONNECTOR_TOKEN = ?"
            val selectionArgs = arrayOf(connectionToken)
            return db.query(
                TABLE_TOKENS,
                projection,
                selection,
                selectionArgs,
                null, null, null
            ).use {
                val distribCol = it.getColumnIndex(FIELD_DISTRIBUTOR)
                val instanceCol = it.getColumnIndex(FIELD_INSTANCE)
                if (it.moveToFirst()
                    && distribCol >= 0
                    && instanceCol >= 0) {
                    Connection.Instance(
                        it.getString(distribCol),
                        it.getString(instanceCol)
                    )
                } else {
                    null
                }
            }
        }

        /**
         * Set or update [instance]
         *
         * @param keyManager should not be null except in rare case (migration)
         * @param overrideTokens should stay empty except in rare case (migration)
         *
         * @return a set of [Connection.Registration] with the existing or created tokens for all
         * distributors (primary/fallbacks)
         */
        fun set(
            instance: String,
            messageForDistributor: String?,
            vapid: String?,
            keyManager: KeyManager?,
            overrideTokens: Set<Connection.Token> = emptySet(),
        ): Set<Connection.Registration> {
            val db = writableDatabase
            return db.runTransaction {
                val values = ContentValues().apply {
                    put(FIELD_INSTANCE, instance)
                    putNullable(FIELD_MESSAGE, messageForDistributor)
                    putNullable(FIELD_VAPID, vapid)
                }
                val tokens = registrations.listToken(instance, db).associate {
                    it.distributor to it.token
                }.toMutableMap()
                // If the registration with this instance already exists,
                // we replace the record:
                // the connection token is removed from TABLE_REGISTRATIONS with the cascade,
                // So we always insert the connection token again, no matter if it is a new one
                db.insertWithOnConflict(
                    TABLE_REGISTRATIONS,
                    null,
                    values,
                    SQLiteDatabase.CONFLICT_REPLACE
                )
                keyManager?.run {
                    if (!exists(instance)) generate(instance)
                }

                /**
                 * If there are override tokens, used during migration from shared prefs
                 */
                overrideTokens.forEach { t ->
                    if (tokens[t.distributor]?.equals(t.token) != true) {
                        saveToken(instance, t.distributor, t.token, db)
                        tokens[t.distributor] = t.token
                    }
                }

                this@DBStore.distributor.list().filter { d ->
                    tokens.keys.none { it == d.packageName }
                }.forEach { d ->
                    tokens[d.packageName] = newToken(instance, d.packageName, db)
                }

                val registration = RegistrationData(instance, messageForDistributor, vapid)

                return@runTransaction tokens.map { t ->
                    Connection.Registration(
                        t.key,
                        t.value,
                        registration
                    )
                }.toSet()
            }
        }

        /**
         * Remove [instance]
         *
         * @return list of remaining instances
         */
        fun remove(
            instance: String,
            keyManager: KeyManager,
        ): Set<String> {
            val db = writableDatabase
            return db.runTransaction {
                val selection = "$FIELD_INSTANCE = ?"
                val selectionArgs = arrayOf(instance)
                // This will delete the instance from TABLE_TOKEN too
                db.delete(TABLE_REGISTRATIONS, selection, selectionArgs)
                keyManager.delete(instance)
                return@runTransaction listInstances(db)
            }
        }

        /**
         * Remove all instances
         */
        fun removeAll(
            keyManager: KeyManager,
        ) {
            val db = writableDatabase
            db.runTransaction {
                listInstances(db).forEach {
                    keyManager.delete(it)
                }
                // this will delete all the connection token from TABLE_TOKEN
                db.delete(TABLE_REGISTRATIONS, null, null)
            }
        }

        /**
         * List all registrations
         */
        fun list(db: SQLiteDatabase = readableDatabase): Set<RegistrationData> {
            return db.query(TABLE_REGISTRATIONS, null, null, null, null, null, null)
                .use {
                    val instanceCol = it.getColumnIndex(FIELD_INSTANCE)
                    val msgCol = it.getColumnIndex(FIELD_MESSAGE)
                    val vapidCol = it.getColumnIndex(FIELD_VAPID)
                    if (instanceCol >= 0 ||
                        msgCol >= 0 ||
                        vapidCol >= 0) {
                        generateSequence {
                            if (it.moveToNext()) it else null
                        }.map { r ->
                            RegistrationData(
                                r.getString(instanceCol),
                                r.getNullableString(msgCol),
                                r.getNullableString(vapidCol)
                            )
                        }.toSet()
                    } else {
                        emptySet()
                    }
                }
        }

        /**
         * List all instances
         *
         * @return set of instances (String)
         */
        fun listInstances(db: SQLiteDatabase = readableDatabase): Set<String> {
            val projection = arrayOf(FIELD_INSTANCE)
            return db.query(TABLE_REGISTRATIONS, projection, null, null, FIELD_INSTANCE, null, null)
                .use {
                    val col = it.getColumnIndex(FIELD_INSTANCE)
                    if (col >= 0) {
                        generateSequence {
                            if (it.moveToNext()) it else null
                        }.map { c->
                            c.getString(col)
                        }.toSet()
                    } else{
                        emptySet()
                    }
                }
        }

        /**
         * List all distrib + token for an instance if given
         *
         * @return set of [Connection.Token]
         */
        fun listToken(
            instance: String?,
            db: SQLiteDatabase = readableDatabase
        ): Set<Connection.Token> {
            val selection = instance?.let { "$FIELD_INSTANCE = ?" }
            val selectionArg = instance?.let { arrayOf(it) }
            val projection = arrayOf(FIELD_DISTRIBUTOR, FIELD_CONNECTOR_TOKEN)
            return db.query(
                TABLE_TOKENS,
                projection,
                selection,
                selectionArg,
                null, null, null
            ).use {
                val distribCol = it.getColumnIndex(FIELD_DISTRIBUTOR)
                val tokenCol = it.getColumnIndex(FIELD_CONNECTOR_TOKEN)
                if (distribCol >= 0 && tokenCol >= 0) {
                    generateSequence {
                        if (it.moveToNext()) it else null
                    }.map { r ->
                        Connection.Token(
                            it.getString(distribCol),
                            it.getString(tokenCol)
                        )
                    }.toSet()
                } else {
                    emptySet()
                }
            }
        }
    }

    inner class KeyStore() {
        fun get(instance: String): WebPushKeysRecord? {
            val db = readableDatabase
            val selection = "$FIELD_INSTANCE = ?"
            val selectionArgs = arrayOf(instance)
            db.query(
                TABLE_KEYS,
                null,
                selection,
                selectionArgs,
                null,
                null,
                null
            ).use {
                it.moveToFirst() || return null
                val authColumn = it.getColumnIndex(FIELD_AUTH)
                val pubKeyColumn = it.getColumnIndex(FIELD_PUBKEY)
                val privKeyColumn = it.getColumnIndex(FIELD_PRIVKEY)
                val ivColumn = it.getColumnIndex(FIELD_IV)
                val auth = if (authColumn >= 0) it.getString(authColumn) else return null
                val pubKey = if (pubKeyColumn >= 0) it.getString(pubKeyColumn) else return null
                val privKey = if (privKeyColumn >= 0) it.getString(privKeyColumn) else return null
                val iv = if (ivColumn >= 0) it.getString(ivColumn) else null
                return WebPushKeysRecord(
                    instance,
                    auth,
                    pubKey,
                    privKey,
                    iv
                )
            }
        }

        /**
         * Update or insert values from [record]
         *
         * If a record already contain keys for the instance, it is replaced.
         */
        fun set(record: WebPushKeysRecord) {
            val db = writableDatabase
            val values = ContentValues().apply {
                put(FIELD_INSTANCE, record.instance)
                put(FIELD_AUTH, record.auth)
                put(FIELD_PUBKEY, record.pubKey)
                put(FIELD_PRIVKEY, record.privKey)
                putNullable(FIELD_IV, record.iv)
            }
            db.insertWithOnConflict(
                TABLE_KEYS,
                null,
                values,
                SQLiteDatabase.CONFLICT_REPLACE
                )
        }

        /**
         * Remove keys for [instance]
         */
        fun remove(instance: String) {
            val db = writableDatabase
            val selection = "$FIELD_INSTANCE = ?"
            val selectionArgs = arrayOf(instance)
            db.delete(TABLE_KEYS, selection, selectionArgs)
        }
    }

    private fun ContentValues.putNullable(key: String, value: String?) {
        value?.let { put(key, it) } ?: putNull(key)
    }

    private fun Cursor.getNullableString(col: Int): String? {
        return if (isNull(col)) null
        else getString(col)
    }

    private fun <T> SQLiteDatabase.runTransaction(block: () -> T): T {
        beginTransaction()
        try {
            val ret = block()
            setTransactionSuccessful()
            return ret
        } finally {
            endTransaction()
        }
    }

    /**
     * Trying to set a cyclic fallback chain
     *
     * Distributor D1 fallbacks to D2 and D2 tries to fallback to D1
     */
    class CyclicFallbackException: AndroidException()

    companion object {
        private var instance: DBStore? = null

        fun get(context: Context) = synchronized(this) {
            instance ?: DBStore(context.applicationContext).also { db ->
                Log.d(TAG, "DBStore version $DB_VERSION")
                db.migrateFromLegacy(context)
                instance = db
            }
        }
        private const val DB_NAME = "unifiedpush-connector"
        private const val DB_VERSION = 1

        /**
         * distributors:
         * - [FIELD_DISTRIBUTOR] String, key
         * - [FIELD_FALLBACK_FROM] String, ref distributor, on delete=cascade
         * - [FIELD_FALLBACK_TO] String, ref distributor, on delete=set null
         * - [FIELD_ACK] Boolean
         */
        private const val TABLE_DISTRIBUTORS = "distributors"
        private const val FIELD_DISTRIBUTOR = "distributor"

        private const val FIELD_FALLBACK_FROM = "fallback_from"
        private const val FIELD_FALLBACK_TO = "fallback_to"
        private const val FIELD_ACK = "ack"

        /**
         * registrations
         * - [FIELD_INSTANCE] String, key
         * - [FIELD_MESSAGE] String, message for distributor
         * - [FIELD_VAPID] String, VAPID pubkey
         */
        private const val TABLE_REGISTRATIONS = "registrations"
        private const val FIELD_INSTANCE = "instance"
        private const val FIELD_MESSAGE = "message"
        private const val FIELD_VAPID = "vapid"

        /**
         * tokens:
         * - [FIELD_CONNECTOR_TOKEN] String, key
         * - [FIELD_INSTANCE] String, ref [TABLE_REGISTRATIONS].instance
         * - [FIELD_DISTRIBUTOR] String, ref [TABLE_DISTRIBUTORS].distributor
         */
        private const val TABLE_TOKENS = "tokens"
        private const val FIELD_CONNECTOR_TOKEN = "connectorToken"

        /**
         * keys: for the [org.unifiedpush.android.connector.keys.DefaultKeyManager]
         * - instance String, key, ref [TABLE_REGISTRATIONS].instance
         * - auth, String
         * - pubkey, String
         * - privkey, String
         * - iv, String, for encrypted privkey (SDK 23+)
         */
        private const val TABLE_KEYS = "keys"
        private const val FIELD_AUTH = "auth"
        private const val FIELD_PUBKEY = "pubkey"
        private const val FIELD_PRIVKEY = "privkey"
        private const val FIELD_IV = "iv"

        /**
         * DO NOT EDIT! It is better to always run all the upgrades
         */
        private const val CREATE_TABLE_DISTRIBUTORS = "CREATE TABLE $TABLE_DISTRIBUTORS (" +
                "$FIELD_DISTRIBUTOR TEXT PRIMARY KEY," +
                "$FIELD_FALLBACK_FROM TEXT," +
                "$FIELD_FALLBACK_TO TEXT," +
                "$FIELD_ACK INTEGER," +
                "FOREIGN KEY ($FIELD_FALLBACK_FROM)" +
                    " REFERENCES $TABLE_DISTRIBUTORS($FIELD_DISTRIBUTOR) ON DELETE CASCADE," +
                "FOREIGN KEY ($FIELD_FALLBACK_TO)" +
                    " REFERENCES $TABLE_DISTRIBUTORS($FIELD_DISTRIBUTOR) ON DELETE SET NULL" +
                ");"

        /**
         * DO NOT EDIT! It is better to always run all the upgrades
         */
        private const val CREATE_TABLE_REGISTRATIONS = "CREATE TABLE $TABLE_REGISTRATIONS (" +
                "$FIELD_INSTANCE TEXT PRIMARY KEY," +
                "$FIELD_MESSAGE TEXT," +
                "$FIELD_VAPID TEXT" +
                ");"

        /**
         * DO NOT EDIT! It is better to always run all the upgrades
         */
        private const val CREATE_TABLE_TOKENS = "CREATE TABLE $TABLE_TOKENS (" +
                "$FIELD_CONNECTOR_TOKEN TEXT PRIMARY KEY," +
                "$FIELD_INSTANCE TEXT," +
                "$FIELD_DISTRIBUTOR TEXT," +
                "FOREIGN KEY ($FIELD_INSTANCE)" +
                    " REFERENCES $TABLE_REGISTRATIONS($FIELD_INSTANCE) ON DELETE CASCADE," +
                "FOREIGN KEY ($FIELD_DISTRIBUTOR)" +
                    " REFERENCES $TABLE_DISTRIBUTORS($FIELD_DISTRIBUTOR) ON DELETE CASCADE" +
                ");"

        /**
         * DO NOT EDIT! It is better to always run all the upgrades
         */
        private const val CREATE_TABLE_KEYS = "CREATE TABLE $TABLE_KEYS (" +
                "$FIELD_INSTANCE TEXT PRIMARY KEY," +
                "$FIELD_AUTH TEXT," +
                "$FIELD_PUBKEY TEXT," +
                "$FIELD_PRIVKEY TEXT," +
                "$FIELD_IV TEXT," +
                "FOREIGN KEY ($FIELD_INSTANCE)" +
                " REFERENCES $TABLE_REGISTRATIONS($FIELD_INSTANCE) ON DELETE CASCADE" +
                ");"

    }

}