<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Util;

/**
 * Sanitises text received from a TiKV or PD server before it is embedded in
 * an exception message (SEC-11, issue #314).
 *
 * A compromised or malicious server controls the error strings it returns.
 * Interpolating them verbatim into an exception message lets it inject
 * newlines, carriage returns and ANSI escape sequences into the client's log
 * records: forged log lines, terminal escape sequences and payloads that
 * confuse structured log parsers. The message is also unbounded, so a server
 * can inflate every log record with an arbitrarily long string.
 *
 * {@see self::sanitise()} removes complete ANSI control sequences and the
 * remaining C0 control characters (including `\n`, `\r`, `\t` and `ESC`) and
 * DEL, then keeps at most {@see self::DEFAULT_MAX_LENGTH} bytes and appends
 * `...` when it truncates. Removal is byte-oriented so invalid UTF-8 from a
 * hostile server is not silently discarded; for valid UTF-8 the cut backs off
 * to a codepoint boundary, so the result stays valid UTF-8.
 *
 * The sanitised text is for exception messages only. A caller that needs the
 * raw server string for protocol decisions must keep using the original value,
 * the way {@see \CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException}
 * keeps `rawKey` out of its message and exposes it only through
 * `getRawKey()`.
 */
final class ServerText
{
    /**
     * Maximum number of bytes of server text kept before the `...` marker.
     * Long enough for a real TiKV error, short enough that a hostile server
     * cannot flood the log sink through one exception.
     */
    public const DEFAULT_MAX_LENGTH = 512;

    /** Appended when a message is truncated, so the reader knows it is partial. */
    private const TRUNCATION_SUFFIX = '...';

    /**
     * Make a server-supplied string safe to embed in a single-line message.
     *
     * Complete ANSI CSI sequences (`ESC [ ... final-byte`) are removed first,
     * so the printable tail of a colour code does not survive as ordinary
     * text. Every remaining C0 control character and DEL is then removed,
     * which also covers ESC sequences the CSI pattern does not match (OSC,
     * single-character escapes, ...). When the result exceeds $maxLength it is
     * cut to at most $maxLength bytes, backed off to a UTF-8 codepoint
     * boundary for valid input, and marked with an ellipsis.
     */
    public static function sanitise(string $text, int $maxLength = self::DEFAULT_MAX_LENGTH): string
    {
        // \e is PCRE's ESC. The byte classes are the standard CSI grammar:
        // parameter bytes 0x30-0x3F, intermediate bytes 0x20-0x2F, final
        // byte 0x40-0x7E.
        $clean = preg_replace('/\e\[[0-?]*[ -\/]*[@-~]/', '', $text) ?? '';

        // Byte-oriented on purpose: /u would return null for invalid UTF-8
        // and silently discard an otherwise usable message.
        $clean = preg_replace('/[\x00-\x1F\x7F]/', '', $clean) ?? '';

        if (strlen($clean) <= $maxLength) {
            return $clean;
        }

        $truncated = substr($clean, 0, $maxLength);

        // A byte cut can land inside a multibyte UTF-8 codepoint, which would
        // leave an invalid sequence in the log record. For valid UTF-8 back
        // off by at most three bytes; invalid UTF-8 keeps the byte cut so its
        // bytes are not silently dropped.
        if (preg_match('//u', $clean) === 1) {
            while ($truncated !== '' && preg_match('//u', $truncated) !== 1) {
                $truncated = substr($truncated, 0, -1);
            }
        }

        return $truncated . self::TRUNCATION_SUFFIX;
    }
}
