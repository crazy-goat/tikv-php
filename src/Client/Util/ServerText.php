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
 * DEL, then caps the result at {@see self::DEFAULT_MAX_LENGTH} bytes. The
 * replacement is byte-oriented, so it never mangles a multibyte UTF-8
 * sequence the way a `/u` pattern would when the server sends invalid UTF-8.
 *
 * The sanitised text is for exception messages only. A caller that needs the
 * raw server string for protocol decisions must keep using the original
 * value, exactly as {@see \CrazyGoat\TiKV\Client\Exception\PdException} keeps
 * `pdErrorMessage` alongside its sanitised message.
 */
final class ServerText
{
    /**
     * Maximum number of bytes kept from a server-supplied string. Long
     * enough for a real TiKV error, short enough that a hostile server
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
     * single-character escapes, ...). The result is truncated on a byte
     * boundary and marked with an ellipsis when it exceeds $maxLength.
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

        return substr($clean, 0, $maxLength) . self::TRUNCATION_SUFFIX;
    }
}
