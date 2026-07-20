package org.unifiedpush.android.connector.internal

import android.app.Activity
import android.content.Intent
import android.graphics.Color
import android.graphics.Typeface
import android.graphics.drawable.ColorDrawable
import android.os.Build
import android.os.Bundle
import android.view.Gravity
import android.widget.ImageView
import android.widget.LinearLayout
import android.widget.Space
import android.widget.TextView
import org.unifiedpush.android.connector.LinkActivityHelper
import org.unifiedpush.android.connector.R

/**
 * @hide
 *
 * Activity declared to start link activity onCreate and store the Auth token.
 *
 * It sends [LinkActivityHelper.startLinkActivityForResult] onCreate
 * and run [LinkActivityHelper.onLinkActivityResult] onActivityResult.
 *
 * It runs [callback] as soon as the interaction with the potential distributor
 * finished.
 */
internal class LinkActivity : Activity() {
    private val helper = LinkActivityHelper(this)

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        window.setBackgroundDrawable(ColorDrawable(0x99000000.toInt()))
        setContent()
        if (intent.action == LinkActivityHelper.ACTION_PICK) {
            if (!helper.startPickLinkActivityForResult()) {
                stop(false)
            }
        } else {
            if (!helper.startLinkActivityForResult()) {
                stop(false)
            }
        }
    }

    private fun setContent() {
        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.MATCH_PARENT
            )
        }

        val textView = TextView(this).apply {
            text = "UnifiedPush"
            textSize = 24f
            setTextColor(Color.WHITE)
            typeface = Typeface.DEFAULT_BOLD
            gravity = Gravity.CENTER
        }
        root.addView(
            Space(this),
            LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
                2f
            )
        )
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP) {
            root.addView(
                ImageView(this).apply {
                    setImageDrawable(getDrawable(R.drawable.ic_unifiedpush))
                },
                LinearLayout.LayoutParams(
                    LinearLayout.LayoutParams.MATCH_PARENT,
                    0,
                    2f
                )
            )
        }
        root.addView(
            textView,
            LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.WRAP_CONTENT,
                LinearLayout.LayoutParams.WRAP_CONTENT,
                1f
            ).apply { gravity = Gravity.CENTER_HORIZONTAL }
        )
        root.addView(
            Space(this),
            LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
                10f
            )
        )
        setContentView(root)
    }

    override fun onActivityResult(
        requestCode: Int,
        resultCode: Int,
        data: Intent?,
    ) {
        stop(
            helper.onLinkActivityResult(requestCode, resultCode, data),
        )
    }

    private fun stop(success: Boolean) {
        callback(success)
        finish()
    }

    internal companion object {
        var callback: (Boolean) -> Unit = {}
    }
}
