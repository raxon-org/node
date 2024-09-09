<?php

namespace Raxon\Node\Trait\Data;

use Raxon\Module\Controller;
use Raxon\Module\Core;
use Raxon\Module\Data as Storage;

use Raxon\Node\Service\Security;

use Exception;

trait Property {

    /**
     * @throws Exception
     */
    public function property_delete($class, $role, $node=[], $options=[]): false | array
    {
        $name = Controller::name($class);
        $object = $this->object();
        if (!array_key_exists('function', $options)) {
            $options['function'] = 'patch'; //this one need the same rights as patch
        }
        if (!array_key_exists('import', $options)) {
            $options['import'] = false;
        }
        $options['relation'] = false;
        if (!Security::is_granted(
            $class,
            $role,
            $options
        )) {
            return false;
        }
        $transaction = $object->config('node.transaction.' . $name);
        if (
            $options['import'] === false &&
            empty($transaction)
        ) {
            $this->lock($class, $options);
        }
        $dir_data = $object->config('project.dir.node') .
            'Data' .
            $object->config('ds');
        $url = $dir_data .
            $name .
            $object->config('extension.json');
        $dir_validate = $object->config('project.dir.node') .
            'Validate' .
            $object->config('ds');
        $validate_url =
            $dir_validate .
            $name .
            $object->config('extension.json');

        $start = microtime(true);
        if ($transaction === true) {
            $data = $object->data_read($url, sha1($url));
        } else {
            $data = $object->data_read($url);
        }
        if ($data) {
            $list = $data->get($name);
            if (!is_array($list)) {
                throw new Exception('Array expected');
            }
        } else {
            $list = [];
        }
        $uuid = [];
        foreach ($list as $nr => $record) {
            $uuid[$record->uuid] = $nr;
        }
        $error = [];
        if (
            is_object($node) &&
            get_class($node) === Storage::class
        ) {
            $node = $node->data();
        } else {
            $node = Core::object($node, Core::OBJECT_OBJECT);
        }
        $record = (object)[];
        if (property_exists($node, 'uuid')) {
            if (array_key_exists($node->uuid, $uuid)) {
                $list_nr = $uuid[$node->uuid];
                $record = $list[$list_nr];
            } else {
                return false;
            }
        } else {
            return false;
        }
        $object->request('node', Core::object_merge($record, $node));
        $object->request('node.#class', $name);
        $has_delete = false;
        foreach($node as $attribute => $true){
            if($attribute === 'uuid'){
                continue;
            }
            $has_delete = true;
            $object->request('delete', 'node.' . $attribute);
        }
        if($has_delete === false){
            return false;
        }
        if (
            array_key_exists('validation', $options) &&
            $options['validation'] === false
        ) {
            $validate = (object)['success' => true];
        } else {
            $validate = $this->validate($object, $validate_url, $name . '.create');
        }
        $record = false;
        if ($validate) {
            if ($validate->success === true) {
                $expose = $this->expose_get(
                    $object,
                    $name,
                    $name . '.' . $options['function'] . '.output'
                );
                $node = new Storage();
                $node->data($object->request('node'));
                $node->set('#class', $name);
                if (
                    $expose &&
                    $role
                ) {
                    $node = $this->expose(
                        $node,
                        $expose,
                        $name,
                        $options['function'],
                        $role
                    );
                    $record = $node->data();
                    if (Core::object_is_empty($record)) {
                        throw new Exception('Empty node after expose...');
                    }
                    if (property_exists($record, 'uuid')) {
                        if (array_key_exists($record->uuid, $uuid)) {
                            $list_nr = $uuid[$record->uuid];
                            $list[$list_nr] = $record;
                        }
                    }
                }
            } else {
                $error[] = $validate->test;
            }
        }
        if (!empty($error)) {
            $response = [];
            $response['error'] = $error;
            if (
                $options['import'] === false &&
                empty($transaction)
            ) {
                $this->unlock($class);
            }
            return $response;
        }
        $data->set($name, $list);
        $response = [];
        $response['node'] = $record;
        if ($transaction === true) {
            $object->data(sha1($url), $data);
            $response['transaction'] = true;
        } else {
            $write = $data->write($url);
            $response['byte'] = $write;
            $response['transaction'] = false;
            if ($options['import'] === false) {
                $this->unlock($class);
            }
        }
        $duration = (microtime(true) - $start) * 1000;
        $response['duration'] = $duration;
        return $response;
    }
}