<?php

namespace Raxon\Node\Trait;

use Exception;
use Raxon\App;
use Raxon\Exception\FileWriteException;
use Raxon\Exception\ObjectException;
use Raxon\Module\Cli;
use Raxon\Module\Controller;
use Raxon\Module\Core;
use Raxon\Module\Data as Storage;
use Raxon\Module\Dir;
use Raxon\Module\File;
use Raxon\Module\Sort;
use Raxon\Node\Service\Security;

trait Stats {

    public function stats($class, $response): void
    {
        if(
            $response &&
            array_key_exists('create', $response) &&
            array_key_exists('put', $response) &&
            array_key_exists('patch', $response) &&
            array_key_exists('commit', $response) &&
            array_key_exists('speed', $response['commit']) &&
            array_key_exists('item_per_second', $response)
        ) {
            $total = $response['create'] + $response['put'] + $response['patch'];
            if ($total === 1) {
                echo 'Imported' .
                    ' (create: ' .
                    $response['create'] .
                    ', put: ' .
                    $response['put'] .
                    ', patch: ' .
                    $response['patch'] .
                    ') ' .
                    $total .
                    ' item (' .
                    $class .
                    ') at ' .
                    $response['item_per_second'] .
                    ' items/sec (' .
                    $response['commit']['speed'] . ')' .
                    PHP_EOL;
            } else {
                echo 'Imported' .
                    ' (create: ' .
                    $response['create'] .
                    ', put: ' .
                    $response['put'] .
                    ', patch: ' .
                    $response['patch'] .
                    ') ' .
                    $total .
                    ' items (' .
                    $class .
                    ') at ' .
                    $response['item_per_second'] .
                    ' items/sec (' .
                    $response['commit']['speed'] . ')' .
                    PHP_EOL;
            }
        }
    }
}