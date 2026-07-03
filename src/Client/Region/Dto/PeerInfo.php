<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region\Dto;

final readonly class PeerInfo
{
    public function __construct(
        public int $peerId,
        public int $storeId,
    ) {
    }
}
