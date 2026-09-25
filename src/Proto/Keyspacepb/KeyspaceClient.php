<?php
// GENERATED CODE -- DO NOT EDIT!

namespace CrazyGoat\Proto\Keyspacepb;

/**
 * Keyspace provides services to manage keyspaces.
 */
class KeyspaceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * @param \CrazyGoat\Proto\Keyspacepb\LoadKeyspaceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function LoadKeyspace(\CrazyGoat\Proto\Keyspacepb\LoadKeyspaceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/keyspacepb.Keyspace/LoadKeyspace',
        $argument,
        ['\CrazyGoat\Proto\Keyspacepb\LoadKeyspaceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * WatchKeyspaces first return all current keyspaces' metadata as its first response.
     * Then, it returns responses containing keyspaces that had their metadata changed.
     * @param \CrazyGoat\Proto\Keyspacepb\WatchKeyspacesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function WatchKeyspaces(\CrazyGoat\Proto\Keyspacepb\WatchKeyspacesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/keyspacepb.Keyspace/WatchKeyspaces',
        $argument,
        ['\CrazyGoat\Proto\Keyspacepb\WatchKeyspacesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \CrazyGoat\Proto\Keyspacepb\UpdateKeyspaceStateRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function UpdateKeyspaceState(\CrazyGoat\Proto\Keyspacepb\UpdateKeyspaceStateRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/keyspacepb.Keyspace/UpdateKeyspaceState',
        $argument,
        ['\CrazyGoat\Proto\Keyspacepb\UpdateKeyspaceStateResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \CrazyGoat\Proto\Keyspacepb\GetAllKeyspacesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function GetAllKeyspaces(\CrazyGoat\Proto\Keyspacepb\GetAllKeyspacesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/keyspacepb.Keyspace/GetAllKeyspaces',
        $argument,
        ['\CrazyGoat\Proto\Keyspacepb\GetAllKeyspacesResponse', 'decode'],
        $metadata, $options);
    }

}
