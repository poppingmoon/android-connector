package org.unifiedpush.android.connector.internal

import android.content.ContentValues
import android.content.Context
import android.database.Cursor
import android.database.sqlite.SQLiteConstraintException
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper
import android.util.Log
import org.unifiedpush.android.connector.TAG
import org.unifiedpush.android.connector.internal.data.Distributor
import org.unifiedpush.android.connector.internal.data.Registration
import org.unifiedpush.android.connector.internal.data.WebPushKeysRecord
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
        //TODO: Migration from SharedPrefs
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
            distributor.set(it.packageName)
            if (it.ack) distributor.ack()
            return@migrateDistributor true
        }
        val distrib = distributor.get()
        store.migrateRegistrations { reg ->
            distrib?.let {
                registrations.set(
                    reg.instance,
                    reg.messageForDistributor,
                    reg.vapid,
                    distrib,
                    null
                )
            }
            // TODO Migrate KeyManager
            return@migrateRegistrations true
        }
    }

    inner class DistributorStore() {

        /**
         * Change primary distributor, remove the previous one
         * and fallback distributors.
         *
         * The connection tokens for the previous distributor(s) are
         * wiped,
         *
         * Does nothing if the distributor is already saved as a primary distrib
         *
         * If the distributor is known as a fallback one, update it to make it primary
         *
         * The new distributor isn't acknowledged yet and don't have
         * any fallback.
         */
        fun set(distributor: String) {
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
            db.runTransaction {
                val projection = arrayOf(FIELD_FALLBACK_FROM)
                val selection = "$FIELD_DISTRIBUTOR = ?"
                val selectionArgs = arrayOf(distributor)
                val exists = db.query(
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
                    // 1. Update the fallback distrib to be a primary distrib (fallback_from = NULL)
                    val values = ContentValues().apply {
                        putNull(FIELD_FALLBACK_FROM)
                    }
                    var selection = "$FIELD_DISTRIBUTOR = ?"
                    val selectionArgs = arrayOf(distributor)
                    db.update(TABLE_DISTRIBUTORS, values, selection, selectionArgs)

                    // 2. Remove the previous primary distrib and its fallbacks (with the cascade)
                    selection = "$FIELD_DISTRIBUTOR != ? AND $FIELD_FALLBACK_FROM is NULL"
                    db.delete(TABLE_DISTRIBUTORS, selection, selectionArgs)
                } else {
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
            }
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
                it.distributor()
            }
        }

        /**
         * Set distributor ack to true
         */
        fun ack() {
            val db = writableDatabase
            val selection = "$FIELD_FALLBACK_TO IS NULL"
            val values = ContentValues().apply {
                put(FIELD_ACK, true)
            }
            db.update(TABLE_DISTRIBUTORS, values, selection, null)
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
         * Get [Distributor] from a query cursor
         */
        private fun Cursor.distributor(): Distributor? {
            moveToFirst() || return null
            val distribColumn = getColumnIndex(FIELD_DISTRIBUTOR)
            val ackColumn = getColumnIndex(FIELD_ACK)
            val fallbackFromColumn = getColumnIndex(FIELD_FALLBACK_FROM)
            val fallbackToColumn = getColumnIndex(FIELD_FALLBACK_TO)

            val packageName = (
                    if (distribColumn >= 0) getString(distribColumn) else null
                    ) ?: return null
            val ack = if (ackColumn >= 0) getInt(ackColumn) != 0 else false
            val fallbackFrom = if (fallbackFromColumn >= 0) getString(fallbackFromColumn) else null
            val fallbackTo = if (fallbackToColumn >= 0) getString(fallbackToColumn) else null

            return Distributor(packageName, ack, fallbackFrom, fallbackTo)
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
         * Try to get the instance from the [connectionToken]
         */
        fun getInstance(connectionToken: String): String? {
            val db = readableDatabase
            val projection = arrayOf(FIELD_INSTANCE)
            val selection = "$FIELD_CONNECTOR_TOKEN = ?"
            val selectionArgs = arrayOf(connectionToken)
            return db.query(
                TABLE_TOKENS,
                projection,
                selection,
                selectionArgs,
                null,
                null,
                null
            ).use {
                val col = it.getColumnIndex(FIELD_INSTANCE)
                if (it.moveToFirst() && col >= 0) {
                    it.getString(col)
                } else {
                    null
                }
            }
        }

        /**
         * Set or update [instance]
         */
        fun set(
            instance: String,
            messageForDistributor: String?,
            vapid: String?,
            distributor: Distributor,
            keyManager: KeyManager?,
        ): Registration {
            val db = writableDatabase
            return db.runTransaction {
                val values = ContentValues().apply {
                    put(FIELD_INSTANCE, instance)
                    putNullable(FIELD_MESSAGE, messageForDistributor)
                    putNullable(FIELD_VAPID, vapid)
                }
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
                // TODO improve KeyManager ?
                keyManager?.run {
                    if (!exists(instance)) generate(instance)
                }
                val token = getToken(instance, distributor.packageName, db)
                    ?: newToken(instance, distributor.packageName, db)
                return@runTransaction Registration(instance, token, messageForDistributor, vapid)
            }
        }

        /**
         * Remove [instance]
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