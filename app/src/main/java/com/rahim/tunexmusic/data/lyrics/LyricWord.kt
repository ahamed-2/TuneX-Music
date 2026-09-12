package com.rahim.tunexmusic.data.lyrics

/**
 * Represents a single word within a synchronized lyric line.
 */
data class LyricWord(
    val text: String,
    val absoluteStartTimeMs: Long,
    val durationMs: Long,
    val charRange: IntRange = 0..0
)
